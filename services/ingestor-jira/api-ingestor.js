// services/ingestor-jira/api-ingestor.js

const fetch = require('node-fetch');
const { MongoClient } = require('mongodb');

// --- 1. CONFIGURACIÓN ---
const JIRA_URL = "https://ar-telefonicahispam.atlassian.net";
const JIRA_EMAIL = process.env.JIRA_EMAIL;
const API_TOKEN = process.env.API_TOKEN;
const MONGO_URI = process.env.MONGO_URI;
const FILTER_IDS = process.env.JIRA_FILTER_IDS ? process.env.JIRA_FILTER_IDS.split(',') : [];

const DB_NAME = 'power_crm_data';
const COLLECTION_NAME = 'jira_issues';

const JIRA_FIELDS = [
    "summary", "issuetype", "status", "priority", "resolution",
    "assignee", "reporter", "created", "updated", "duedate",
    "project", "sprint", "customfield_10016" 
].join(',');

/**
 * --- CORRECCIÓN 1: Renombramos la función ---
 * Ahora se llama 'ingestJiraData' para coincidir con el script que la llama.
 */
async function ingestJiraData() {
    console.log('🚀 Iniciando ingestor de datos desde la API de Jira...');

    if (!JIRA_EMAIL || !API_TOKEN || !MONGO_URI || FILTER_IDS.length === 0) {
        console.error('❌ Error: Faltan variables de entorno críticas (JIRA_EMAIL, API_TOKEN, MONGO_URI, JIRA_FILTER_IDS).');
        // Devolvemos un error para que el proceso principal se detenga
        throw new Error("Configuración de entorno incompleta.");
    }

    const authHeader = `Basic ${Buffer.from(`${JIRA_EMAIL}:${API_TOKEN}`).toString('base64')}`;
    let allIssues = [];

    for (const filterId of FILTER_IDS) {
        console.log(`➡️  Consultando filtro con ID: ${filterId.trim()}`);
        const issuesFromFilter = await fetchIssuesFromFilter(filterId.trim(), authHeader);
        allIssues.push(...issuesFromFilter);
        console.log(`   ↪️  Se obtuvieron ${issuesFromFilter.length} tickets de este filtro.`);
    }

    if (allIssues.length === 0) {
        console.log('No se encontraron tickets en ningún filtro. Finalizando.');
        return;
    }
    
    const uniqueIssues = deduplicateIssues(allIssues);
    console.log(`\nSe consolidaron ${allIssues.length} registros en ${uniqueIssues.length} tickets únicos.`);

    await uploadToMongo(uniqueIssues);

    console.log('\n✅ ¡Proceso de ingesta por API completado con éxito!');
}

// Las funciones auxiliares (fetchIssuesFromFilter, deduplicateIssues, uploadToMongo)
// permanecen exactamente iguales que en la versión anterior.
// ... (puedes copiar y pegar el resto de las funciones aquí sin cambios)

async function fetchIssuesFromFilter(filterId, authHeader) {
    let allIssues = [];
    let startAt = 0;
    let isLast = false;
    const maxResults = 100;

    while (!isLast) {
        const jql = `filter = ${filterId}`;
        const apiUrl = `${JIRA_URL}/rest/api/3/search?jql=${encodeURIComponent(jql)}&fields=${JIRA_FIELDS}&startAt=${startAt}&maxResults=${maxResults}`;
        
        try {
            const response = await fetch(apiUrl, {
                method: 'GET',
                headers: { 'Authorization': authHeader, 'Accept': 'application/json' }
            });

            if (!response.ok) {
                const errorText = await response.text();
                console.error(`❌ Error al consultar el filtro ${filterId}: ${response.status} - ${errorText}`);
                break;
            }

            const pageData = await response.json();
            
            const cleanedIssues = pageData.issues.map(issue => {
                const sprintInfo = issue.fields.sprint;
                const sprintName = sprintInfo && sprintInfo.length > 0 ? sprintInfo[sprintInfo.length - 1].name : null;

                return {
                    key: issue.key,
                    summary: issue.fields.summary,
                    issue_type: issue.fields.issuetype.name,
                    status: issue.fields.status.name,
                    priority: issue.fields.priority ? issue.fields.priority.name : null,
                    resolution: issue.fields.resolution ? issue.fields.resolution.name : null,
                    assignee: issue.fields.assignee ? issue.fields.assignee.displayName : "No asignado",
                    reporter: issue.fields.reporter ? issue.fields.reporter.displayName : "No asignado",
                    created_at: issue.fields.created,
                    updated_at: issue.fields.updated,
                    due_date: issue.fields.duedate,
                    project_key: issue.fields.project.key,
                    project_name: issue.fields.project.name,
                    sprint: sprintName,
                    story_points: issue.fields.customfield_10016 || null 
                };
            });

            allIssues.push(...cleanedIssues);
            isLast = (pageData.startAt + pageData.issues.length) >= pageData.total;
            startAt += pageData.issues.length;

        } catch (error) {
            console.error(`🚨 Ocurrió un error de conexión consultando el filtro ${filterId}:`, error);
            break;
        }
    }
    return allIssues;
}

function deduplicateIssues(issues) {
    const issueMap = new Map();
    for (const issue of issues) {
        const existingIssue = issueMap.get(issue.key);
        if (!existingIssue || new Date(issue.updated_at) > new Date(existingIssue.updated_at)) {
            issueMap.set(issue.key, issue);
        }
    }
    return Array.from(issueMap.values());
}

async function uploadToMongo(issues) {
    const client = new MongoClient(MONGO_URI);
    try {
        await client.connect();
        const database = client.db(DB_NAME);
        const collection = database.collection(COLLECTION_NAME);
        console.log(`\nConectado a MongoDB. Actualizando ${issues.length} registros...`);

        const bulkOps = issues.map(issue => ({
            updateOne: {
                filter: { key: issue.key },
                update: { $set: issue },
                upsert: true
            }
        }));

        if (bulkOps.length > 0) {
            const result = await collection.bulkWrite(bulkOps);
            console.log(`Resultado: ${result.upsertedCount} insertados, ${result.modifiedCount} actualizados.`);
        }
    } catch (error) {
        console.error('❌ Error al subir los datos a MongoDB:', error);
    } finally {
        await client.close();
    }
}


/**
 * --- CORRECCIÓN 2: Exportamos la función ---
 * Esta línea permite que otros archivos, como 'run-sync.js', puedan
 * importar y usar la función 'ingestJiraData'.
 */
module.exports = ingestJiraData;