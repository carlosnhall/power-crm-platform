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
const RendimientoSchema = new mongoose.Schema({
    Proveedor: String,
    MesConsulta: String,
    TipoReporte: String,
    NUM_INCIDENTE: String,
    GRUPO: String
}, { strict: false, timestamps: true });

const RendimientoConnectis = mongoose.model('RendimientoConnectis', RendimientoSchema, 'rendimiento_connectis');
const RendimientoNttdata = mongoose.model('RendimientoNttdata', RendimientoSchema, 'rendimiento_nttdata');


// --- API ENDPOINTS ---

// Endpoint para GUARDAR los datos de rendimiento
app.post('/api/rendimiento', async (req, res) => {
    const { proveedor, mes, data } = req.body;
    if (!proveedor || !mes || !data) {
        return res.status(400).json({ message: "Faltan los parámetros 'proveedor', 'mes' o 'data'." });
    }
    console.log(`Recibida petición para guardar ${data.length} registros de '${proveedor}' para '${mes}'.`);
    try {
        const collection = proveedor.toUpperCase() === 'CONNECTIS' ? RendimientoConnectis : RendimientoNttdata;
        await collection.deleteMany({ MesConsulta: mes });
        console.log(` -> Datos antiguos para '${mes}' eliminados.`);
        await collection.insertMany(data);
        console.log(` -> ${data.length} nuevos registros insertados.`);
        res.status(201).json({ message: `Datos guardados correctamente.` });
    } catch (error) {
        console.error("❌ Error al guardar en MongoDB:", error);
        res.status(500).json({ message: "Error interno al guardar los datos." });
    }
});

// Endpoint para OBTENER los datos (para el futuro frontend)
app.get('/api/rendimiento/:proveedor', async (req, res) => {
    const { proveedor } = req.params;
    const collection = proveedor.toUpperCase() === 'CONNECTIS' ? RendimientoConnectis : RendimientoNttdata;
    try {
        const data = await collection.find();
        res.status(200).json(data);
    } catch (error) {
        res.status(500).json({ message: "Error al obtener los datos." });
    }
});

// --- ¡NUEVO ENDPOINT! ---
// Para devolver una lista de los meses que ya han sido descargados para un proveedor.
app.get('/api/rendimiento/:proveedor/meses', async (req, res) => {
    const { proveedor } = req.params;
    console.log(`Petición para obtener meses descargados para: ${proveedor}`);
    const collection = proveedor.toUpperCase() === 'CONNECTIS' ? RendimientoConnectis : RendimientoNttdata;
    try {
        // Usamos .distinct() para obtener una lista de valores únicos del campo 'MesConsulta'
        const meses = await collection.distinct('MesConsulta');
        res.status(200).json(meses); // Devuelve un array de strings, ej: ["January", "February"]
    } catch (error) {
        console.error(`Error obteniendo meses para ${proveedor}:`, error);
        res.status(500).json({ message: "Error al obtener los meses." });
    }
});


// --- INICIAR EL SERVIDOR ---
app.listen(PORT, () => {
    console.log(`🚀 Microservicio 'persistence-api' corriendo en el puerto ${PORT}`);
});
