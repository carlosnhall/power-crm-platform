// services/ingestor-individuos/index.js

const axios = require('axios');
const { MongoClient } = require('mongodb');

// --- CONFIGURACIÓN (leída desde las variables de entorno) ---
const GRAFANA_API_URL = process.env.GRAFANA_API_URL;
const GRAFANA_DATASOURCE_ID = parseInt(process.env.GRAFANA_DATASOURCE_ID, 10);
const GRAFANA_ORG_ID = parseInt(process.env.GRAFANA_ORG_ID, 10);
const MONGO_URI = process.env.MONGO_URI;

const DB_NAME = 'power_crm_data';
const COLLECTION_NAME = 'individuos';

/**
 * Función principal que orquesta la ingesta de datos de "individuos".
 */
async function ingestIndividuosData() {
    console.log('--- Iniciando ingestor de datos de Individuos desde Grafana ---');

    if (!GRAFANA_API_URL || !GRAFANA_DATASOURCE_ID || !MONGO_URI) {
        console.error('❌ Error: Faltan variables de entorno críticas para el ingestor de Individuos.');
        return false;
    }

    try {
        // 1. Extraer datos de Grafana
        const grafanaData = await fetchFromGrafana();
        if (!grafanaData || grafanaData.length === 0) {
            console.log('✅ No se encontraron nuevos datos de individuos para procesar.');
            return true;
        }

        console.log(`✅ Se obtuvieron ${grafanaData.length} registros desde Grafana.`);

        // 2. Guardar en MongoDB
        await uploadToMongo(grafanaData);

        console.log('✅ ¡Proceso de ingesta de Individuos completado con éxito!');
        return true;

    } catch (error) {
        console.error('❌ Error en el proceso de ingesta de Individuos:', error);
        return false;
    }
}

/**
 * Realiza la consulta a la API de Grafana para obtener los datos.
 */
async function fetchFromGrafana() {
    const today = new Date();
    const startOfYear = new Date(today.getFullYear(), 0, 1);
    const startTimeMs = startOfYear.getTime();
    const currentTimeMs = today.getTime();

    // Este es el cuerpo de la petición que vimos en tu script de Python
    const payload = {
        from: String(startTimeMs),
        to: String(currentTimeMs),
        queries: [{
            refId: "A",
            datasourceId: GRAFANA_DATASOURCE_ID,
            rawSql: `SELECT INCIDENT_NUMBER as Incidente, DATEADD(SECOND,SUBMIT_DATE - 10800, '01/01/1970') AS 'Fecha', CASE STATUS WHEN 0 THEN 'Nuevo' WHEN 1 THEN 'Asignado' WHEN 2 THEN 'En curso' WHEN 3 THEN 'Pendiente' WHEN 4 THEN 'Resuelto' WHEN 5 THEN 'Cerrado' WHEN 6 THEN 'Cancelado' END Estado, ASSIGNED_GROUP as 'Grupo Asignado', CATEGORIZATION_TIER_1 as 'Cat Op 1', CATEGORIZATION_TIER_2 as 'Cat Op 2', CATEGORIZATION_TIER_3 as 'Cat Op 3', RESOLUTION_CATEGORY as 'Cat Resolución 1', RESOLUTION_CATEGORY_TIER_2 as 'Cat Resolucion 2', RESOLUTION_CATEGORY_TIER_3 as 'Cat Resolucion 3', DETAILED_DECRIPTION AS 'Descripción', RESOLUTION as 'Resolucion' FROM itsm.CabeceraDeIncidenciasEstructuraOriginal WHERE SUBMIT_DATE >= DATEDIFF(s, '1970-01-01 00:00:00', CONVERT(varchar(4), DATEPART(YEAR,GETDATE())) + '-01-01 00:00:00.000') + 10800 AND CATEGORIZATION_TIER_2 = 'Power CRM'`,
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
    const rows = table.rows.map(row => {
        const obj = {};
        columns.forEach((col, i) => {
            obj[col] = row[i];
        });
        return obj;
    });

    return rows;
}

/**
 * Guarda los datos en MongoDB usando "upsert" para evitar duplicados.
 */
async function uploadToMongo(data) {
    const client = new MongoClient(MONGO_URI);
    try {
        await client.connect();
        const database = client.db(DB_NAME);
        const collection = database.collection(COLLECTION_NAME);
        console.log(`\nConectado a MongoDB. Actualizando ${data.length} registros en la colección '${COLLECTION_NAME}'...`);

        const bulkOps = data.map(record => ({
            updateOne: {
                filter: { Incidente: record.Incidente }, // Usamos 'Incidente' como clave única
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

// Exportamos la función principal para que el orquestador pueda llamarla.
module.exports = ingestIndividuosData;