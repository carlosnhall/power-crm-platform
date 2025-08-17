// services/ingestor-jira/api-ingestor.js

const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '.env') });

const fetch = require('node-fetch');
const { MongoClient } = require('mongodb');

// --- CONFIGURACIÓN ---
const JIRA_URL = "https://ar-telefonicahispam.atlassian.net";
const JIRA_EMAIL = process.env.JIRA_EMAIL;
const API_TOKEN = process.env.API_TOKEN;
const MONGO_URI = process.env.MONGO_URI;
const FILTER_IDS = process.env.JIRA_FILTER_IDS ? process.env.JIRA_FILTER_IDS.split(',') : [];

const DB_NAME = 'power_crm_data';
const COLLECTION_NAME = 'jira_issues';

async function main() {
    console.log('🚀 Iniciando ingestor de datos desde la API de Jira...');
    if (!JIRA_EMAIL || !API_TOKEN || !MONGO_URI || FILTER_IDS.length === 0) {
        console.error('❌ Error: Faltan variables de entorno críticas.');
        return;
    }
    const authHeader = `Basic ${Buffer.from(`${JIRA_EMAIL}:${API_TOKEN}`).toString('base64')}`;
    let allIssues = [];
    for (const filterId of FILTER_IDS) {
        const issuesFromFilter = await fetchIssuesFromFilter(filterId.trim(), authHeader);
        allIssues.push(...issuesFromFilter);
    }
    if (allIssues.length === 0) {
        console.log('No se encontraron tickets en ningún filtro. Finalizando.');
        return;
    }
    const uniqueIssues = deduplicateIssues(allIssues);
    await uploadToMongo(uniqueIssues);
    console.log('\n✅ ¡Proceso de ingesta por API completado con éxito!');
}

async function fetchIssuesFromFilter(filterId, authHeader) {
    console.log(`➡️  Consultando filtro con ID: ${filterId}`);
    let allIssues = [];
    let startAt = 0;
    let isLast = false;
    const maxResults = 100;
    
    // --- ¡CAMBIO IMPORTANTE! ---
    // Agregamos los campos de Sprint y Story Points a la lista de campos que pedimos.
    // El nombre exacto como 'customfield_10020' puede variar, pero estos son los más comunes.
    const fields = `summary,issuetype,status,assignee,reporter,priority,project,created,updated,duedate,resolution,customfield_10020,customfield_10028`;

    while (!isLast) {
        const jql = `filter = ${filterId}`;
        const apiUrl = `${JIRA_URL}/rest/api/3/search?jql=${encodeURIComponent(jql)}&startAt=${startAt}&maxResults=${maxResults}&fields=${fields}`;
        
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
            
            // --- ¡AQUÍ PROCESAMOS LOS NUEVOS CAMPOS! ---
            const cleanedIssues = pageData.issues.map(issue => {
                // Extraemos la info del sprint. Viene en un array, tomamos el último.
                const sprintField = issue.fields.customfield_10020;
                const lastSprint = sprintField && sprintField.length > 0 ? sprintField[sprintField.length - 1].name : null;

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
                    sprint: lastSprint, // <-- Nuevo campo
                    story_points: issue.fields.customfield_10028 || 0 // <-- Nuevo campo
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
    console.log(`   ↪️  Se obtuvieron ${allIssues.length} tickets de este filtro.`);
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
    const uniqueIssues = Array.from(issueMap.values());
    console.log(`\nSe consolidaron ${issues.length} registros en ${uniqueIssues.length} tickets únicos.`);
    return uniqueIssues;
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

module.exports = main;

if (require.main === module) {
    main();
}