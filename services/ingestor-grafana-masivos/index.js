// services/ingestor-grafana-masivos/index.js (Versión para PostgreSQL)

require('dotenv').config(); // Aseguramos que lea las variables de entorno
const axios = require('axios');
const { Pool } = require('pg'); // <-- 1. Cambiamos el conector a PostgreSQL

// --- CONFIGURACIÓN ---
const GRAFANA_API_URL = process.env.GRAFANA_MASIVOS_API_URL; 
const GRAFANA_DATASOURCE_ID = parseInt(process.env.GRAFANA_MASIVOS_DATASOURCE_ID, 10);
const GRAFANA_ORG_ID = parseInt(process.env.GRAFANA_MASIVOS_ORG_ID, 10);

// Usamos las variables de entorno de PostgreSQL
const pool = new Pool({
    user: process.env.PG_USER,
    host: process.env.PG_HOST,
    database: process.env.PG_DATABASE,
    password: process.env.PG_PASSWORD,
    port: process.env.PG_PORT,
});

// Usamos el nombre de la tabla que definimos
const DB_TABLE_NAME = 'incidentes_masivos'; 

async function ingestMasivosData() {
    console.log('--- Iniciando ingestor de datos de Masivos desde Grafana para PostgreSQL---');
    if (!GRAFANA_API_URL || !GRAFANA_DATASOURCE_ID) {
        console.error('❌ Error: Faltan variables de entorno críticas para el ingestor de Masivos.');
        return false;
    }
    try {
        const grafanaData = await fetchFromGrafana();
        if (!grafanaData || grafanaData.length === 0) {
            console.log('✅ No se encontraron datos de masivos para procesar.');
            return true;
        }
        console.log(`✅ Se obtuvieron ${grafanaData.length} registros de masivos desde Grafana.`);
        
        // <-- 2. Llamamos a la nueva función de carga
        await uploadToPostgres(grafanaData);
        
        console.log('✅ ¡Proceso de ingesta de Masivos completado con éxito!');
        return true;
    } catch (error) {
        console.error('❌ Error en el proceso de ingesta de Masivos:', error);
        return false;
    }
}

// La función fetchFromGrafana no necesita cambios, ya que obtiene los datos de la misma forma.
async function fetchFromGrafana() {
    // ... (el código anterior de la función no cambia) ...
    const toDate = new Date();
    const fromDate = new Date(toDate.getFullYear(), 0, 1);
    const startTimeMs = fromDate.getTime();
    const currentTimeMs = toDate.getTime();
    const rawSql = `
        SELECT INCIDENT_NUMBER as "Incidente",
        DATEADD(SECOND,SUBMIT_DATE - 10800, '01/01/1970') AS "Fecha",
        CASE STATUS WHEN 0 THEN 'Nuevo' WHEN 1 THEN 'Asignado' WHEN 2 THEN 'En curso' WHEN 3 THEN 'Pendiente' WHEN 4 THEN 'Resuelto' WHEN 5 THEN 'Cerrado' WHEN 6 THEN 'Cancelado' END "Estado",
        ASSIGNED_GROUP as "Grupo Asignado",
        CATEGORIZATION_TIER_1 as "Cat Op 1",
        CATEGORIZATION_TIER_2 as "Cat Op 2",
        CATEGORIZATION_TIER_3 as "Cat Op 3",
        RESOLUTION_CATEGORY as "Cat Resolución 1",
        RESOLUTION_CATEGORY_TIER_2 as "Cat Resolucion 2",
        RESOLUTION_CATEGORY_TIER_3 as "Cat Resolucion 3",
        DETAILED_DECRIPTION AS "Descripción", 
        RESOLUTION as "Resolucion"
        FROM itsm.CabeceraDeIncidenciasEstructuraOriginal
        WHERE
        SUBMIT_DATE >= DATEDIFF(s, '1970-01-01 00:00:00', CONVERT(varchar(4), DATEPART(YEAR,GETDATE())) + '-' + CONVERT(varchar(4), DATEPART(MONTH,GETDATE())) + '-01 00:00:00.000') + 10800
        AND CATEGORIZATION_TIER_2 IN ('Incidentes Masivos','INTRANET / INTERNET CORPORATIVA')
        -- 👇 LÍNEA MODIFICADA 👇
        AND CATEGORIZATION_TIER_3 = 'Power CRM'
    `;
    const payload = { from: String(startTimeMs), to: String(currentTimeMs), queries: [{ refId: "A", datasourceId: GRAFANA_DATASOURCE_ID, rawSql: rawSql, format: "table" }] };
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

// <-- 3. Reemplazamos uploadToMongo con uploadToPostgres
async function uploadToPostgres(records) {
    if (records.length === 0) return;

    const client = await pool.connect();
    console.log(`\nConectado a PostgreSQL. Actualizando ${records.length} registros en la tabla '${DB_TABLE_NAME}'...`);

    // Columnas de la tabla, deben coincidir EXACTAMENTE con las de la DB.
    // Usamos comillas dobles para los nombres con espacios/mayúsculas/acentos.
    const columns = [
        '"Incidente"', '"Fecha"', '"Estado"', '"Grupo Asignado"', '"Cat Op 1"', '"Cat Op 2"', '"Cat Op 3"',
        '"Cat Resolución 1"', '"Cat Resolucion 2"', '"Cat Resolucion 3"', '"Descripción"', '"Resolucion"'
    ];
    
    // Prepara la parte "SET" de la consulta para la actualización en caso de conflicto
    const onConflictUpdate = columns.slice(1).map(col => `${col} = EXCLUDED.${col}`).join(', ');

    try {
        const values = records.map(rec => columns.map(col => rec[col.replace(/"/g, '')]));
        const valuePlaceholders = records.map((_, index) => {
            const base = index * columns.length;
            return `(${columns.map((_, i) => `$${base + i + 1}`).join(', ')})`;
        }).join(', ');

        const query = `
            INSERT INTO ${DB_TABLE_NAME} (${columns.join(', ')})
            VALUES ${valuePlaceholders}
            ON CONFLICT ("Incidente") DO UPDATE SET
            ${onConflictUpdate};
        `;
        
        const result = await client.query(query, values.flat());
        console.log(`Resultado: ${result.rowCount} filas afectadas (insertadas o actualizadas).`);

    } catch (error) {
        console.error('❌ Error al subir datos a PostgreSQL:', error);
    } finally {
        client.release();
    }
}

module.exports = ingestMasivosData;


// --- ¡AGREGÁ ESTA LÍNEA AL FINAL! ---
// Esta línea ejecuta la función principal al correr el script.
//ingestMasivosData();