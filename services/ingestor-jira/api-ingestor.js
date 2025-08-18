// services/ingestor-jira/api-ingestor.js

const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '.env') });

const fetch = require('node-fetch');
const { Pool } = require('pg'); // --- Usamos la librería de PostgreSQL

// --- CONFIGURACIÓN ---
const JIRA_URL = "https://ar-telefonicahispam.atlassian.net";
const JIRA_EMAIL = process.env.JIRA_EMAIL;
const API_TOKEN = process.env.API_TOKEN;
const JIRA_FILTERS = process.env.JIRA_FILTERS ? process.env.JIRA_FILTERS.split(',') : [];

// --- Configuración de PostgreSQL ---
const pool = new Pool({
    user: process.env.PG_USER,
    host: process.env.PG_HOST,
    database: process.env.PG_DATABASE,
    password: process.env.PG_PASSWORD,
    port: process.env.PG_PORT,
});

const DB_TABLE_NAME = 'jira_issues'; // Usamos el nombre de nuestra tabla SQL

// --- NUEVA FUNCIÓN: Transforma el JSON de Jira a nuestro formato SQL ---
function transformarJiraParaSQL(issue, nombreMesa) {
    const fields = issue.fields;
    return {
        issue_id: issue.key,
        issue_url: issue.self,
        summary: fields.summary ?? null,
        description: fields.description ?? null,
        project_name: fields.project?.name ?? null,
        nombre_mesa: nombreMesa,
        status: fields.status?.name ?? null,
        issue_type: fields.issuetype?.name ?? null,
        priority: fields.priority?.name ?? null,
        created_date: fields.created,
        updated_date: fields.updated,
        resolved_date: fields.resolutiondate ?? null,
        due_date: fields.duedate ?? null,
        assignee_name: fields.assignee?.displayName ?? null,
        reporter_name: fields.reporter?.displayName ?? null,
        epic_link_key: fields.parent?.key ?? fields.customfield_10014 ?? null, // Intenta parent (subtarea) o un custom field común para Epic Link
        epic_name: fields.parent?.fields?.summary ?? null, // Similar para el nombre
        sprint_name: fields.customfield_10020?.[0]?.name ?? null,
        sprint_state: fields.customfield_10020?.[0]?.state ?? null,
        sprint_start_date: fields.customfield_10020?.[0]?.startDate ?? null,
        sprint_end_date: fields.customfield_10020?.[0]?.endDate ?? null
    };
}

async function main() {
    console.log('🚀 Iniciando ingestor de datos desde la API de Jira para PostgreSQL...');
    if (!JIRA_EMAIL || !API_TOKEN || JIRA_FILTERS.length === 0) {
        console.error('❌ Error: Faltan variables de entorno críticas (JIRA o PG).');
        return;
    }
    const authHeader = `Basic ${Buffer.from(`${JIRA_EMAIL}:${API_TOKEN}`).toString('base64')}`;
    let allIssues = [];

    for (const filterPair of JIRA_FILTERS) {
        const [filterId, nombreMesa] = filterPair.split(':');
        if (!filterId || !nombreMesa) continue;

        const issuesFromFilter = await fetchIssuesFromFilter(filterId.trim(), nombreMesa.trim(), authHeader);
        allIssues.push(...issuesFromFilter);
    }

    if (allIssues.length === 0) {
        console.log('No se encontraron tickets en ningún filtro. Finalizando.');
        return;
    }

    const uniqueIssues = deduplicateIssues(allIssues);
    await uploadToPostgres(uniqueIssues);
    console.log('\n✅ ¡Proceso de ingesta por API completado con éxito!');
    await pool.end(); // Cerramos la conexión al final
}

async function fetchIssuesFromFilter(filterId, nombreMesa, authHeader) {
    console.log(`➡️  Consultando filtro "${nombreMesa}" (ID: ${filterId})`);
    let allIssues = [];
    let startAt = 0;
    let isLast = false;
    const maxResults = 100;
    
    const fields = '*navigable'; // Pedimos los campos principales

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
            
            const transformedIssues = pageData.issues.map(issue => transformarJiraParaSQL(issue, nombreMesa));

            allIssues.push(...transformedIssues);
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
        const existingIssue = issueMap.get(issue.issue_id);
        if (!existingIssue || new Date(issue.updated_date) > new Date(existingIssue.updated_date)) {
            issueMap.set(issue.issue_id, issue);
        }
    }
    const uniqueIssues = Array.from(issueMap.values());
    console.log(`\nSe consolidaron ${issues.length} registros en ${uniqueIssues.length} tickets únicos.`);
    return uniqueIssues;
}

// --- VERSIÓN MEJORADA: Sube los datos a PostgreSQL por lotes ---
async function uploadToPostgres(issues) {
    if (issues.length === 0) return;

    const client = await pool.connect();
    console.log(`\nConectado a PostgreSQL. Actualizando ${issues.length} registros en lotes...`);

    const BATCH_SIZE = 500; // Procesaremos de 500 en 500
    let totalAffectedRows = 0;

    // Columnas de nuestra tabla, en orden
    const columns = [
        'issue_id', 'issue_url', 'summary', 'description', 'project_name', 'nombre_mesa',
        'status', 'issue_type', 'priority', 'created_date', 'updated_date', 'resolved_date',
        'due_date', 'assignee_name', 'reporter_name', 'epic_link_key', 'epic_name',
        'sprint_name', 'sprint_state', 'sprint_start_date', 'sprint_end_date'
    ];
    const onConflictUpdate = columns.slice(1).map(col => `${col} = EXCLUDED.${col}`).join(', ');

    try {
        for (let i = 0; i < issues.length; i += BATCH_SIZE) {
            const batch = issues.slice(i, i + BATCH_SIZE);
            console.log(` -> Procesando lote: ${i + 1} a ${i + batch.length} de ${issues.length}`);

            const values = batch.map(issue => columns.map(col => issue[col]));
            
            const valuePlaceholders = batch.map((_, index) => {
                const base = index * columns.length;
                return `(${columns.map((_, i) => `$${base + i + 1}`).join(', ')})`;
            }).join(', ');

            const query = `
                INSERT INTO ${DB_TABLE_NAME} (${columns.join(', ')})
                VALUES ${valuePlaceholders}
                ON CONFLICT (issue_id) DO UPDATE SET
                ${onConflictUpdate};
            `;
            
            const result = await client.query(query, values.flat());
            totalAffectedRows += result.rowCount;
        }
        console.log(`\nResultado final: ${totalAffectedRows} filas afectadas en total (insertadas o actualizadas).`);
    } catch (error) {
        console.error('❌ Error al subir un lote de datos a PostgreSQL:', error);
    } finally {
        client.release();
    }
}

if (require.main === module) {
    main();
}