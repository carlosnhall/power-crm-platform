const axios = require('axios');
const { MongoClient } = require('mongodb');

// --- CONFIGURACIÓN ---
const GRAFANA_API_URL = process.env.GRAFANA_API_URL;
const GRAFANA_DATASOURCE_ID = parseInt(process.env.GRAFANA_DATASOURCE_ID, 10);
const GRAFANA_ORG_ID = parseInt(process.env.GRAFANA_ORG_ID, 10);
const MONGO_URI = process.env.MONGO_URI;

const DB_NAME = 'power_crm_data';
const COLLECTION_NAME = 'individuos';

async function ingestIndividuosData() {
    console.log('--- Iniciando ingestor de datos de Individuos desde Grafana ---');
    if (!GRAFANA_API_URL || !GRAFANA_DATASOURCE_ID || !MONGO_URI) {
        console.error('❌ Error: Faltan variables de entorno críticas.');
        return false;
    }

    try {
        const grafanaData = await fetchFromGrafana();
        if (!grafanaData || grafanaData.length === 0) {
            console.log('✅ No se encontraron datos de individuos para procesar.');
            return true;
        }

        // --- NUEVO PASO DE DIAGNÓSTICO ---
        console.log(`\n--- DIAGNÓSTICO DE DUPLICADOS ---`);
        const totalRegistros = grafanaData.length;
        const incidentesUnicos = new Set(grafanaData.map(record => record.Incidente));
        console.log(`Registros totales recibidos de Grafana: ${totalRegistros}`);
        console.log(`Incidentes únicos encontrados: ${incidentesUnicos.size}`);
        console.log(`Cantidad de registros duplicados: ${totalRegistros - incidentesUnicos.size}`);
        console.log(`------------------------------------`);
        // --- FIN DEL DIAGNÓSTICO ---
        
        await uploadToMongo(grafanaData);
        console.log('✅ ¡Proceso de ingesta de Individuos completado con éxito!');
        return true;

    } catch (error) {
        console.error('❌ Error en el proceso de ingesta de Individuos:', error);
        return false;
    }
}

// ... (El resto de las funciones fetchFromGrafana y uploadToMongo no necesitan cambios) ...
async function fetchFromGrafana() {
    const today = new Date();
    const startOfYear = new Date(today.getFullYear(), 0, 1);
    const startTimeMs = startOfYear.getTime();
    const currentTimeMs = today.getTime();
    const payload = {
        from: String(startTimeMs), to: String(currentTimeMs),
        queries: [{ refId: "A", datasourceId: GRAFANA_DATASOURCE_ID, rawSql: `SELECT INCIDENT_NUMBER as Incidente, DATEADD(SECOND,SUBMIT_DATE - 10800, '01/01/1970') AS 'Fecha', CASE STATUS WHEN 0 THEN 'Nuevo' WHEN 1 THEN 'Asignado' WHEN 2 THEN 'En curso' WHEN 3 THEN 'Pendiente' WHEN 4 THEN 'Resuelto' WHEN 5 THEN 'Cerrado' WHEN 6 THEN 'Cancelado' END Estado, ASSIGNED_GROUP as 'Grupo Asignado', CATEGORIZATION_TIER_1 as 'Cat Op 1', CATEGORIZATION_TIER_2 as 'Cat Op 2', CATEGORIZATION_TIER_3 as 'Cat Op 3', RESOLUTION_CATEGORY as 'Cat Resolución 1', RESOLUTION_CATEGORY_TIER_2 as 'Cat Resolucion 2', RESOLUTION_CATEGORY_TIER_3 as 'Cat Resolucion 3', DETAILED_DECRIPTION AS 'Descripción', RESOLUTION as 'Resolucion' FROM itsm.CabeceraDeIncidenciasEstructuraOriginal WHERE SUBMIT_DATE >= DATEDIFF(s, '1970-01-01 00:00:00', CONVERT(varchar(4), DATEPART(YEAR,GETDATE())) + '-01-01 00:00:00.000') + 10800 AND CATEGORIZATION_TIER_2 = 'Power CRM'`, format: "table" }]
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

async function uploadToMongo(data) {
    const client = new MongoClient(MONGO_URI);
    try {
        await client.connect();
        const database = client.db(DB_NAME);
        const collection = database.collection(COLLECTION_NAME);
        console.log(`\nConectado a MongoDB. Actualizando ${data.length} registros en la colección '${COLLECTION_NAME}'...`);
        const bulkOps = data.map(record => ({
            updateOne: { filter: { Incidente: record.Incidente }, update: { $set: record }, upsert: true }
        }));
        if (bulkOps.length > 0) {
            const result = await collection.bulkWrite(bulkOps);
            console.log(`Resultado: ${result.upsertedCount} insertados, ${result.modifiedCount} actualizados.`);
        }
    } finally {
        await client.close();
    }
}

module.exports = ingestIndividuosData;