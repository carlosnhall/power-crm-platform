// Importar las librerías necesarias
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
require('dotenv').config();

// --- CONFIGURACIÓN ---
const app = express();
const PORT = process.env.PORT || 3003;
const MONGO_URI = process.env.MONGO_URI;

app.use(cors());
app.use(express.json({ limit: '50mb' }));

// --- CONEXIÓN A MONGODB ---
mongoose.connect(MONGO_URI)
    .then(() => console.log('✅ Conectado a MongoDB Atlas'))
    .catch(err => console.error('❌ Error conectando a MongoDB:', err));

// --- MODELOS DE DATOS (SCHEMAS) ---
const RendimientoSchema = new mongoose.Schema({ /* ... */ }, { strict: false, timestamps: true });
const RendimientoConnectis = mongoose.model('RendimientoConnectis', RendimientoSchema, 'rendimiento_connectis');
const RendimientoNttdata = mongoose.model('RendimientoNttdata', RendimientoSchema, 'rendimiento_nttdata');

// --- ¡NUEVO! Schema para los datos de Jira ---
const JiraIssueSchema = new mongoose.Schema({
    key: { type: String, unique: true, required: true }, // La clave del issue (ej. AGPAPP-588)
    project: String,
    summary: String,
    type: String,
    status: String,
    priority: String,
    assignee: String,
    labels: [String],
    sprint: Object,
    created: Date,
    updated: Date,
}, { strict: false, timestamps: true }); // `strict: false` permite guardar otros campos también

const JiraIssue = mongoose.model('JiraIssue', JiraIssueSchema, 'jira_issues');


// --- API ENDPOINTS ---

// Endpoints para Rendimiento (sin cambios)
// ... app.post('/api/rendimiento', ...)
// ... app.get('/api/rendimiento/:proveedor/meses', ...)


// --- ¡NUEVOS ENDPOINTS PARA JIRA! ---

// Endpoint para GUARDAR los datos de Jira (con lógica de actualización)
app.post('/api/jira-issues', async (req, res) => {
    const issues = req.body; // Esperamos un array de issues
    if (!issues || !Array.isArray(issues)) {
        return res.status(400).json({ message: "El cuerpo de la petición debe ser un array de issues." });
    }
    console.log(`Recibida petición para guardar/actualizar ${issues.length} issues de Jira.`);

    try {
        const bulkOps = issues.map(issue => ({
            updateOne: {
                filter: { key: issue.key }, // Busca el issue por su clave única
                update: { $set: issue },   // Actualiza todos sus campos
                upsert: true               // Si no lo encuentra, lo crea (insert)
            }
        }));

        if (bulkOps.length > 0) {
            const result = await JiraIssue.bulkWrite(bulkOps);
            console.log(` -> Resultado: ${result.upsertedCount} creados, ${result.modifiedCount} actualizados.`);
        }
        
        res.status(201).json({ message: `Issues de Jira guardados/actualizados correctamente.` });
    } catch (error) {
        console.error("❌ Error al guardar en MongoDB:", error);
        res.status(500).json({ message: "Error interno al guardar los datos de Jira." });
    }
});

// Endpoint para OBTENER la fecha del último issue actualizado
app.get('/api/jira-issues/latest-update', async (req, res) => {
    try {
        // Busca en la colección, ordena por 'updated' de forma descendente y toma el primero.
        const latestIssue = await JiraIssue.findOne().sort({ updated: -1 });
        if (latestIssue) {
            res.status(200).json({ latestUpdate: latestIssue.updated });
        } else {
            res.status(200).json({ latestUpdate: null }); // No hay datos todavía
        }
    } catch (error) {
        res.status(500).json({ message: "Error al obtener la última fecha de actualización." });
    }
});


// --- INICIAR EL SERVIDOR ---
app.listen(PORT, () => {
    console.log(`🚀 Microservicio 'persistence-api' corriendo en el puerto ${PORT}`);
});
