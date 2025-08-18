// Importar las librerías necesarias
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg'); // Usamos el conector de PostgreSQL
require('dotenv').config();

// --- CONFIGURACIÓN ---
const app = express();
const PORT = process.env.PORT || 3003;

// Configuración del Pool de Conexiones a PostgreSQL
const pool = new Pool({
    user: process.env.PG_USER,
    host: process.env.PG_HOST,
    database: process.env.PG_DATABASE,
    password: process.env.PG_PASSWORD,
    port: process.env.PG_PORT,
});

app.use(cors());
app.use(express.json({ limit: '50mb' }));

const DB_TABLE_NAME = 'jira_issues';

// --- API ENDPOINTS PARA JIRA / POSTGRESQL ---

// Endpoint para GUARDAR los datos de Jira (con lógica de actualización - "Upsert")
app.post('/api/jira-issues', async (req, res) => {
    const issues = req.body; // Esperamos un array de issues
    if (!issues || !Array.isArray(issues)) {
        return res.status(400).json({ message: "El cuerpo de la petición debe ser un array de issues." });
    }
    console.log(`Recibida petición para guardar/actualizar ${issues.length} issues de Jira en PostgreSQL.`);

    // Reutilizamos la misma lógica de carga por lotes del ingestor
    const client = await pool.connect();
    const BATCH_SIZE = 500;
    let totalAffectedRows = 0;

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
        
        console.log(` -> Resultado: ${totalAffectedRows} filas afectadas.`);
        res.status(201).json({ message: `Issues de Jira guardados/actualizados correctamente.`, affectedRows: totalAffectedRows });

    } catch (error) {
        console.error("❌ Error al guardar en PostgreSQL:", error);
        res.status(500).json({ message: "Error interno al guardar los datos de Jira." });
    } finally {
        client.release();
    }
});

// Endpoint para OBTENER la fecha del último issue actualizado
app.get('/api/jira-issues/latest-update', async (req, res) => {
    try {
        const query = `
            SELECT updated_date FROM ${DB_TABLE_NAME}
            ORDER BY updated_date DESC NULLS LAST
            LIMIT 1;
        `;
        const result = await pool.query(query);

        if (result.rows.length > 0) {
            res.status(200).json({ latestUpdate: result.rows[0].updated_date });
        } else {
            res.status(200).json({ latestUpdate: null }); // No hay datos todavía
        }
    } catch (error) {
        console.error("❌ Error al obtener la última fecha de actualización:", error);
        res.status(500).json({ message: "Error al obtener la última fecha de actualización." });
    }
});


// --- INICIAR EL SERVIDOR ---
app.listen(PORT, async () => {
    // Probamos la conexión a PostgreSQL al iniciar
    try {
        const client = await pool.connect();
        console.log('✅ Conectado a PostgreSQL');
        client.release();
    } catch (err) {
        console.error('❌ Error conectando a PostgreSQL:', err);
    }
    console.log(`🚀 Microservicio 'persistence-api' (versión PostgreSQL) corriendo en el puerto ${PORT}`);
});