// services/ingestor-jira/api-ingestor.js (VERSIÓN DE DIAGNÓSTICO)

const fetch = require('node-fetch');

// --- 1. CONFIGURACIÓN ---
const JIRA_URL = "https://ar-telefonicahispam.atlassian.net";
const JIRA_EMAIL = process.env.JIRA_EMAIL;
const API_TOKEN = process.env.API_TOKEN;
const FILTER_IDS = process.env.JIRA_FILTER_IDS ? process.env.JIRA_FILTER_IDS.split(',') : [];

async function ingestJiraData() {
    console.log('🕵️  Iniciando modo de diagnóstico para los campos de Jira...');

    if (!JIRA_EMAIL || !API_TOKEN || FILTER_IDS.length === 0) {
        console.error('❌ Error: Faltan variables de entorno críticas.');
        throw new Error("Configuración de entorno incompleta.");
    }

    const authHeader = `Basic ${Buffer.from(`${JIRA_EMAIL}:${API_TOKEN}`).toString('base64')}`;
    const filterId = FILTER_IDS[0].trim();

    const apiUrl = `${JIRA_URL}/rest/api/3/search?jql=filter%20%3D%20${filterId}&maxResults=1&fields=*all`;
    
    console.log(`➡️  Consultando 1 issue del filtro ${filterId} para ver su estructura...`);

    try {
        const response = await fetch(apiUrl, {
            method: 'GET',
            headers: { 'Authorization': authHeader, 'Accept': 'application/json' }
        });

        if (!response.ok) {
            console.error(`❌ Error al conectar con la API de Jira: ${response.status} - ${await response.text()}`);
            throw new Error("Fallo en la conexión con la API de Jira.");
        }

        const data = await response.json();

        if (data.issues && data.issues.length > 0) {
            const firstIssueFields = data.issues[0].fields;
            console.log("\n✅ ¡Respuesta recibida! Abajo están TODOS los campos del primer issue encontrado.");
            console.log("--- COPIAR DESDE AQUÍ ---");
            console.log(JSON.stringify(firstIssueFields, null, 2));
            console.log("--- COPIAR HASTA AQUÍ ---");
            console.log("\nPor favor, copia el bloque JSON de arriba y pégalo en nuestra conversación.");
        } else {
            console.log("⚠️ No se encontraron issues en el filtro especificado.");
        }

    } catch (error) {
        console.error('🚨 Ocurrió un error en el diagnóstico:', error);
        throw error;
    } finally {
        console.log("Diagnóstico finalizado. Saliendo del proceso.");
        process.exit(0);
    }
}

module.exports = ingestJiraData;