require('dotenv').config();
const axios = require('axios');
const { Pool } = require('pg');

// ... (toda la configuración de variables de entorno y el pool de PG se mantiene igual) ...
const GRAFANA_API_URL = process.env.GRAFANA_API_URL;
const GRAFANA_DATASOURCE_ID = parseInt(process.env.GRAFANA_DATASOURCE_ID, 10);
const GRAFANA_ORG_ID = parseInt(process.env.GRAFANA_ORG_ID, 10);

const pool = new Pool({
    user: process.env.PG_USER,
    host: process.env.PG_HOST,
    database: process.env.PG_DATABASE,
    password: process.env.PG_PASSWORD,
    port: process.env.PG_PORT,
});

const DB_TABLE_NAME = '"incidentesIndividuales"';

// ... (la función transformarKeysParaSQL se mantiene igual) ...
function transformarKeysParaSQL(record) {
    return {
        incidente: record.Incidente,
        fecha: record.Fecha,
        estado: record.Estado,
        grupo_asignado: record['Grupo Asignado'],
        cat_op_1: record['Cat Op 1'],
        cat_op_2: record['Cat Op 2'],
        cat_op_3: record['Cat Op 3'],
        cat_resolucion_1: record['Cat Resolución 1'],
        cat_resolucion_2: record['Cat Resolucion 2'],
        cat_resolucion_3: record['Cat Resolucion 3'],
        descripcion: record.Descripción,
        resolucion: record.Resolucion,
    };
}

async function ingestIndividuosData() {
    // --- LOG DE INICIO ---
    console.log('[DEBUG] 1. Iniciando la función ingestIndividuosData...');
    
    if (!GRAFANA_API_URL || !GRAFANA_DATASOURCE_ID) {
        console.error('❌ Error: Faltan variables de entorno críticas de Grafana o PostgreSQL.');
        return false;
    }
    // --- LOG DE VARIABLES ---
    console.log(`[DEBUG] 2. GRAFANA_API_URL = ${GRAFANA_API_URL}`);

    try {
        // --- LOG ANTES DE LA LLAMADA ---
        console.log('[DEBUG] 3. Llamando a fetchFromGrafana...');
        const grafanaData = await fetchFromGrafana();
        
        // --- LOG DESPUÉS DE LA LLAMADA ---
        console.log(`[DEBUG] 4. Se recibieron ${grafanaData ? grafanaData.length : 0} registros de Grafana.`);

        if (!grafanaData || grafanaData.length === 0) {
            console.log('✅ No se encontraron datos de individuos para procesar. Finalizando.');
            return true;
        }

        const datosTransformados = grafanaData.map(transformarKeysParaSQL);
        
        // --- LOG ANTES DE LA CARGA A DB ---
        console.log('[DEBUG] 5. Llamando a uploadToPostgres...');
        await uploadToPostgres(datosTransformados);
        
        // --- LOG DE ÉXITO FINAL ---
        console.log('[DEBUG] 6. Proceso finalizado con éxito.');
        return true;

    } catch (error) {
        // --- LOG DE ERROR DETALLADO ---
        console.error('❌ Error en el bloque principal de ingestIndividuosData:', error);
        return false;
    }
}

// ... (la función fetchFromGrafana se mantiene igual) ...
async function fetchFromGrafana() {
    // ...
    const today = new Date();
    const startOfYear = new Date(today.getFullYear(), 0, 1);
    const startTimeMs = startOfYear.getTime();
    const currentTimeMs = today.getTime();
    const payload = {
        from: String(startTimeMs), to: String(currentTimeMs),
        queries: [{ refId: "A", datasourceId: GRAFANA_DATASOURCE_ID, rawSql: `SELECT INCIDENT_NUMBER as Incidente, DATEADD(SECOND,SUBMIT_DATE - 10800, '01/01/1970') AS 'Fecha', CASE STATUS WHEN 0 THEN 'Nuevo' WHEN 1 THEN 'Asignado' WHEN 2 THEN 'En curso' WHEN 3 THEN 'Pendiente' WHEN 4 THEN 'Resuelto' WHEN 5 THEN 'Cerrado' WHEN 6 THEN 'Cancelado' END Estado, ASSIGNED_GROUP as 'Grupo Asignado', CATEGORIZATION_TIER_1 as 'Cat Op 1', CATEGORIZATION_TIER_2 as 'Cat Op 2', CATEGORIZATION_TIER_3 as 'Cat Op 3', RESOLUTION_CATEGORY as 'Cat Resolución 1', CATEGORIZATION_TIER_2 as 'Cat Resolucion 2', RESOLUTION_CATEGORY_TIER_3 as 'Cat Resolucion 3', DETAILED_DECRIPTION AS 'Descripción', RESOLUTION as 'Resolucion' FROM itsm.CabeceraDeIncidenciasEstructuraOriginal WHERE SUBMIT_DATE >= DATEDIFF(s, '1970-01-01 00:00:00', CONVERT(varchar(4), DATEPART(YEAR,GETDATE())) + '-01-01 00:00:00.000') + 10800 AND CATEGORIZATION_TIER_2 = 'Power CRM'`, format: "table" }]
    };
    const headers = { "Content-Type": "application/json", "Accept": "application/json", "X-Grafana-Org-Id": String(GRAFANA_ORG_ID) };
    const response = await axios.post(GRAFANA_API_URL, payload, { headers });
    const results = response.data.results.A;
    if (!results || !results.tables || results.tables.length === 0) { return []; }
    const table = results.tables[0];
    const columns = table.columns.map(c => c.text);
    return table.rows.map(row => {
        const obj = {};
        columns.forEach((col, i) => { obj[col] = row[i]; });
        return obj;
    });
}

// ... (la función uploadToPostgres se mantiene igual) ...
async function uploadToPostgres(records) {
    if (records.length === 0) return;

    const client = await pool.connect();
    console.log(`[DEBUG] Conectado a PostgreSQL. Intentando actualizar ${records.length} registros...`);

    const columns = [
        'incidente', 'fecha', 'estado', 'grupo_asignado', 'cat_op_1', 'cat_op_2', 'cat_op_3',
        'cat_resolución_1', 'cat_resolucion_2', 'cat_resolucion_3', 'descripción', 'resolucion'
    ];
    
    const onConflictUpdate = columns.slice(1).map(col => `${col} = EXCLUDED.${col}`).join(', ');

    try {
        const values = records.map(rec => columns.map(col => rec[col]));
        const valuePlaceholders = records.map((_, index) => {
            const base = index * columns.length;
            return `(${columns.map((_, i) => `$${base + i + 1}`).join(', ')})`;
        }).join(', ');

        const query = `
            INSERT INTO ${DB_TABLE_NAME} (${columns.join(', ')})
            VALUES ${valuePlaceholders}
            ON CONFLICT (incidente) DO UPDATE SET
            ${onConflictUpdate};
        `;
        
        console.log('[DEBUG] Ejecutando la consulta SQL...');
        const result = await client.query(query, values.flat());
        console.log(`[DEBUG] Consulta finalizada. ${result.rowCount} filas afectadas.`);

    } catch (error) {
        console.error('❌ Error al subir datos a PostgreSQL:', error);
    } finally {
        client.release();
    }
}


// --- Ejecución del script ---
ingestIndividuosData();

module.exports = ingestIndividuosData;