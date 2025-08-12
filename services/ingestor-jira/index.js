// Importar las librerías necesarias
const express = require('express');
const axios = require('axios');
require('dotenv').config();

// --- CONFIGURACIÓN ---
const app = express();
const PORT = process.env.PORT || 3004;

const JIRA_BASE_URL = process.env.JIRA_BASE_URL;
const JIRA_USER_EMAIL = process.env.JIRA_USER_EMAIL;
const JIRA_API_TOKEN = process.env.JIRA_API_TOKEN;
const JIRA_PROJECT_KEYS = process.env.JIRA_PROJECT_KEYS;
const PERSISTENCE_API_URL = process.env.PERSISTENCE_API_URL; // URL del servicio de persistencia

// --- CLIENTE DE AXIOS PRECONFIGURADO ---
const jiraApi = axios.create({ /* ... */ });


/**
 * Obtiene solo los issues nuevos o actualizados desde la última sincronización.
 */
async function fetchIncrementalIssues() {
    // 1. Preguntar al servicio de persistencia por la última fecha de actualización
    let lastUpdate = null;
    try {
        console.log("Consultando última fecha de sincronización...");
        const response = await axios.get(`${PERSISTENCE_API_URL}/api/jira-issues/latest-update`);
        lastUpdate = response.data.latestUpdate;
        if (lastUpdate) {
            console.log(` -> Última actualización encontrada: ${lastUpdate}`);
        } else {
            console.log(" -> No se encontraron datos previos. Se realizará una carga completa.");
        }
    } catch (error) {
        console.error("Advertencia: No se pudo conectar al servicio de persistencia para obtener la fecha. Se realizará una carga completa.");
    }

    // 2. Construir la consulta JQL dinámica
    const projectKeysForQuery = JIRA_PROJECT_KEYS.split(',').map(key => `"${key.trim()}"`).join(', ');
    let jql = `project IN (${projectKeysForQuery})`;
    if (lastUpdate) {
        // Formateamos la fecha para que Jira la entienda (YYYY-MM-DD HH:mm)
        const formattedDate = new Date(lastUpdate).toISOString().slice(0, 16).replace('T', ' ');
        jql += ` AND updated >= "${formattedDate}"`;
    }
    jql += ' ORDER BY updated ASC'; // Ordenamos por fecha para procesar en orden

    console.log(`Ejecutando JQL: ${jql}`);
    
    // 3. Bucle para obtener todas las páginas de resultados (igual que antes)
    let allIssues = [];
    // ... (la lógica de paginación es la misma que en el script anterior)
    
    console.log(`✅ Se encontraron ${allIssues.length} issues nuevos o actualizados.`);
    return allIssues;
}


// --- API ENDPOINT PRINCIPAL ---
app.post('/trigger-ingest', async (req, res) => {
    try {
        const newIssues = await fetchIncrementalIssues();

        const cleanedData = newIssues.map(issue => ({ /* ... (lógica de limpieza de datos) ... */ }));

        // 4. Enviar los datos nuevos al servicio de persistencia para guardarlos
        if (cleanedData.length > 0) {
            console.log(`📤 Enviando ${cleanedData.length} issues al servicio de persistencia...`);
            await axios.post(`${PERSISTENCE_API_URL}/api/jira-issues`, cleanedData);
            console.log("✅ Datos enviados a persistencia correctamente.");
        } else {
            console.log("ℹ️ No se encontraron issues nuevos para guardar.");
        }

        res.status(200).json({
            message: `Ingesta de Jira completada. Se procesaron ${cleanedData.length} issues nuevos/actualizados.`,
            sample: cleanedData.slice(0, 5)
        });

    } catch (error) {
        res.status(500).json({ message: error.message });
    }
});


// --- INICIAR EL SERVIDOR ---
app.listen(PORT, () => {
    console.log(`🚀 Microservicio 'ingestor-jira' corriendo en el puerto ${PORT}`);
});
