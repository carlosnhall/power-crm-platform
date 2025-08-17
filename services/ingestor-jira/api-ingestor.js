// services/ingestor-jira/api-ingestor.js (VERSIÓN DE DIAGNÓSTICO 2.0 - CORREGIDA)

const fetch = require('node-fetch');

// --- 1. CONFIGURACIÓN ---
const JIRA_URL = "https://ar-telefonicahispam.atlassian.net";
const JIRA_EMAIL = process.env.JIRA_EMAIL;
const API_TOKEN = process.env.API_TOKEN;
const FILTER_IDS = process.env.JIRA_FILTER_IDS ? process.env.JIRA_FILTER_IDS.split(',') : [];

/**
 * --- Función de diagnóstico ---
 * Se exporta con el nombre correcto para que 'run-sync.js' la pueda llamar.
 */
async function ingestJiraData() {
    console.log('🕵️  Iniciando modo de diagnóstico para los campos de Jira...');

    if (!JIRA_EMAIL || !API_TOKEN || FILTER_IDS.length === 0) {
        console.error('❌ Error: Faltan variables de entorno críticas (JIRA_EMAIL, API_TOKEN, JIRA_FILTER_IDS).');
        // Lanzamos un error para detener el proceso de automatización
        throw new Error("Configuración de entorno incompleta.");
    }

    const authHeader = `Basic ${Buffer.from(`${JIRA_EMAIL}:${API_TOKEN}`).toString('base64')}`;
    const filterId = FILTER_IDS[0].trim(); // Usaremos solo el primer filtro

    // Pedimos a la API que nos devuelva TODOS los campos
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
        // Lanzamos el error para que el runner de GitHub lo capture
        throw error;
    } finally {
        // Detenemos el proceso exitosamente después del diagnóstico
        console.log("Diagnóstico finalizado. Saliendo del proceso.");
        process.exit(0);
    }
}

// Exportamos la función para que 'run-sync.js' la pueda encontrar
module.exports = ingestJiraData;