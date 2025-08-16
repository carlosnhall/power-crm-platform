// services/ingestor-grafana-masivos/index.js

const axios = require('axios');
const { MongoClient } = require('mongodb');

// --- CONFIGURACIÓN ---
const GRAFANA_API_URL = process.env.GRAFANA_MASIVOS_API_URL; 
const GRAFANA_DATASOURCE_ID = parseInt(process.env.GRAFANA_MASIVOS_DATASOURCE_ID, 10);
const GRAFANA_ORG_ID = parseInt(process.env.GRAFANA_MASIVOS_ORG_ID, 10);
const MONGO_URI = process.env.MONGO_URI;

const DB_NAME = 'power_crm_data';
const COLLECTION_NAME = 'masivos';

async function ingestMasivosData() {
    console.log('--- Iniciando ingestor de datos de Masivos desde Grafana ---');
    if (!GRAFANA_API_URL || !GRAFANA_DATASOURCE_ID || !MONGO_URI) {
        console.error('❌ Error: Faltan variables de entorno críticas para el ingestor de Masivos.');
        return false;
    }
    try {
        const grafanaData = await fetchFromGrafana();
        if (!grafanaData || grafanaData.length === 0) {
            console.log('✅ No se encontraron nuevos datos de masivos para procesar.');
            return true;
        }
        console.log(`✅ Se obtuvieron ${grafanaData.length} registros de masivos desde Grafana.`);
        await uploadToMongo(grafanaData);
        console.log('✅ ¡Proceso de ingesta de Masivos completado con éxito!');
        return true;
    } catch (error) {
        console.error('❌ Error en el proceso de ingesta de Masivos:', error);
        return false;
    }
}

async function fetchFromGrafana() {
    // --- ¡AQUÍ ESTÁ LA CORRECCIÓN! ---
    // Volvemos a la lógica de pedir los datos desde el inicio del año actual.
    const toDate = new Date();
    const fromDate = new Date(toDate.getFullYear(), 0, 1); // Primer día del AÑO actual

    const startTimeMs = fromDate.getTime();
    const currentTimeMs = toDate.getTime();
    
    // La columna SUBMIT_DATE es un timestamp de Unix (segundos).
    const fromTimestamp = Math.floor(startTimeMs / 1000);

    // La consulta SQL ahora filtra desde el inicio del año.
    const rawSql = `SELECT INCIDENT_NUMBER as Incidente, DATEADD(SECOND,SUBMIT_DATE - 10800, '01/01/1970') AS 'Fecha', STATUS FROM itsm.CabeceraDeIncidenciasEstructuraOriginal WHERE CATEGORIZATION_TIER_1 = 'Masivos' AND SUBMIT_DATE >= ${fromTimestamp}`;

    const payload = {
        from: String(startTimeMs),
        to: String(currentTimeMs),
        queries: [{
            refId: "A",
            datasourceId: GRAFANA_DATASOURCE_ID,
            rawSql: rawSql,
            format: "table"
        }]
    };
    const headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Grafana-Org-Id": String(GRAFANA_ORG_ID)
    };
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

async function uploadToMongo(data) {
    const client = new MongoClient(MONGO_URI);
    try {
        await client.connect();
        const database = client.db(DB_NAME);
        const collection = database.collection(COLLECTION_NAME);
        console.log(`\nConectado a MongoDB. Actualizando ${data.length} registros en la colección '${COLLECTION_NAME}'...`);
        const bulkOps = data.map(record => ({
            updateOne: {
                filter: { Incidente: record.Incidente },
                update: { $set: record },
                upsert: true
            }
        }));
        if (bulkOps.length > 0) {
            const result = await collection.bulkWrite(bulkOps);
            console.log(`Resultado: ${result.upsertedCount} insertados, ${result.modifiedCount} actualizados.`);
        }
    } finally {
        await client.close();
    }
}

module.exports = ingestMasivosData;