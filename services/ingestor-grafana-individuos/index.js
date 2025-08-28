// El llamado a dotenv.config() debe ser la PRIMERA línea
require('dotenv').config();

const axios = require('axios');
const { Pool } = require('pg');

// --- 1. CONFIGURACIÓN ---
// Las variables se leen del archivo .env
const GRAFANA_API_URL = process.env.GRAFANA_API_URL;
const GRAFANA_DATASOURCE_ID = parseInt(process.env.GRAFANA_DATASOURCE_ID, 10);
const GRAFANA_ORG_ID = parseInt(process.env.GRAFANA_ORG_ID, 10);
const DB_TABLE_NAME = process.env.DB_TABLE_NAME || 'incidentes_individuales';

// Pool de conexiones a PostgreSQL
const pool = new Pool({
    user: process.env.PG_USER,
    host: process.env.PG_HOST,
    database: process.env.PG_DATABASE,
    password: process.env.PG_PASSWORD,
    port: process.env.PG_PORT,
});


// --- 2. FUNCIONES ---

/**
 * Obtiene los datos de incidentes desde la API de Grafana.
 */
async function fetchFromGrafana() {
    const today = new Date();
    const startOfYear = new Date(today.getFullYear(), 0, 1);
    const payload = {
        from: String(startOfYear.getTime()),
        to: String(today.getTime()),
        queries: [{
            refId: "A",
            datasourceId: GRAFANA_DATASOURCE_ID,
            rawSql: `SELECT INCIDENT_NUMBER as "Incidente", DATEADD(SECOND,SUBMIT_DATE - 10800, '01/01/1970') AS "Fecha", CASE STATUS WHEN 0 THEN 'Nuevo' WHEN 1 THEN 'Asignado' WHEN 2 THEN 'En curso' WHEN 3 THEN 'Pendiente' WHEN 4 THEN 'Resuelto' WHEN 5 THEN 'Cerrado' WHEN 6 THEN 'Cancelado' END AS "Estado", ASSIGNED_GROUP as "Grupo Asignado", CATEGORIZATION_TIER_1 as "Cat Op 1", CATEGORIZATION_TIER_2 as "Cat Op 2", CATEGORIZATION_TIER_3 as "Cat Op 3", RESOLUTION_CATEGORY as "Cat Resolución 1", RESOLUTION_CATEGORY_TIER_2 as "Cat Resolucion 2", RESOLUTION_CATEGORY_TIER_3 as "Cat Resolucion 3", DETAILED_DECRIPTION AS "Descripción", RESOLUTION as "Resolucion" FROM itsm.CabeceraDeIncidenciasEstructuraOriginal WHERE SUBMIT_DATE >= DATEDIFF(s, '1970-01-01 00:00:00', CONVERT(varchar(4), DATEPART(YEAR,GETDATE())) + '-01-01 00:00:00.000') + 10800 AND CATEGORIZATION_TIER_2 = 'Power CRM'`,
            format: "table"
        }]
    };
    const headers = { "Content-Type": "application/json", "Accept": "application/json", "X-Grafana-Org-Id": String(GRAFANA_ORG_ID) };

    const response = await axios.post(GRAFANA_API_URL, payload, { headers });
    const results = response.data.results.A;
    if (!results || !results.tables || results.tables.length === 0) {
        return [];
    }
    const table = results.tables[0];
    const columns = table.columns.map(c => c.text);
    return table.rows.map(row => {
        const obj = {};
        columns.forEach((col, i) => { obj[col] = row[i]; });
        return obj;
    });
}

/**
 * Transforma las claves de un objeto para que coincidan con el esquema de la base de datos.
 * @param {object} record El objeto con claves originales (ej: "Grupo Asignado").
 * @returns {object} El objeto con claves limpias (ej: "grupo_asignado").
 */
function transformarKeysParaSQL(record) {
    const newRecord = {};
    for (const key in record) {
        const newKey = key.trim().toLowerCase()
            .replace(/ /g, '_')
            .replace(/ó/g, 'o').replace(/á/g, 'a').replace(/é/g, 'e').replace(/í/g, 'i').replace(/ú/g, 'u');
        newRecord[newKey] = record[key];
    }
    return newRecord;
}


/**
 * Sube los registros a PostgreSQL usando una consulta parametrizada y una transacción.
 * @param {Array<object>} records Un array de objetos de incidentes ya transformados.
 */
async function uploadToPostgres(records) {
    if (!records || records.length === 0) {
        console.log('ℹ️ No hay registros para subir a la base de datos.');
        return;
    }

    const client = await pool.connect();
    console.log(`[DEBUG] Conectado a PostgreSQL. Intentando actualizar ${records.length} registros...`);

    try {
        await client.query('BEGIN');

        const columns = Object.keys(records[0]);
        const onConflictUpdate = columns.slice(1).map(col => `"${col}" = EXCLUDED."${col}"`).join(', ');

        const queryText = `
            INSERT INTO "${DB_TABLE_NAME}" ("${columns.join('", "')}")
            SELECT * FROM UNNEST ($1::text[], $2::timestamp[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[], $9::text[], $10::text[], $11::text[], $12::text[])
            ON CONFLICT (incidente) DO UPDATE SET
            ${onConflictUpdate};
        `;
        
        // Preparamos los arrays de valores para la consulta
        const valuesByColumn = columns.map(col => records.map(rec => rec[col]));

        console.log('[DEBUG] Ejecutando la consulta SQL en bloque...');
        const result = await client.query(queryText, valuesByColumn);
        console.log(`[DEBUG] Consulta finalizada. ${result.rowCount !== null ? result.rowCount : 'Múltiples'} filas afectadas.`);

        await client.query('COMMIT');
        console.log('[DEBUG] Transacción confirmada.');

    } catch (error) {
        await client.query('ROLLBACK');
        console.error('❌ Error al subir datos a PostgreSQL:', error);
        throw error;
    } finally {
        client.release();
        console.log('[DEBUG] Conexión a PostgreSQL liberada.');
    }
}


/**
 * Función principal que orquesta el proceso de extracción y carga.
 */
async function ingestIndividuosData() {
    console.log('[DEBUG] 1. Iniciando la función ingestIndividuosData...');
    try {
        console.log('[DEBUG] 3. Llamando a fetchFromGrafana...');
        const grafanaData = await fetchFromGrafana();
        console.log(`[DEBUG] 4. Se recibieron ${grafanaData.length} registros de Grafana.`);

        if (grafanaData.length === 0) {
            console.log('✅ No se encontraron nuevos datos para procesar.');
            return;
        }

        const datosTransformados = grafanaData.map(transformarKeysParaSQL);
        
        console.log('[DEBUG] 5. Llamando a uploadToPostgres...');
        await uploadToPostgres(datosTransformados);
        
        console.log('✅ Proceso finalizado con éxito.');

    } catch (error) {
        console.error('❌ Error en el proceso principal de ingestión:', error.message);
    }
}
module.exports = ingestIndividuosData;

// --- 3. EJECUCIÓN DEL SCRIPT ---
ingestIndividuosData().then(() => {
    console.log("Script terminado.");
    pool.end(); // Cierra todas las conexiones del pool al finalizar
});