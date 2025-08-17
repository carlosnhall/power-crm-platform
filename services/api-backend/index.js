// services/api-backend/index.js

require('dotenv').config();
const express = require('express');
const { MongoClient } = require('mongodb');
const cors = require('cors');

// --- CONFIGURACIÓN ---
const app = express();
const PORT = process.env.PORT || 3005;
const MONGO_URI = process.env.MONGO_URI;

const DB_NAME = 'power_crm_data';
const JIRA_COLLECTION = 'jira_issues';
const RENDIMIENTO_COLLECTION = 'grafana_rendimiento';
const INDIVIDUOS_COLLECTION = 'individuos';
const MASIVOS_COLLECTION = 'masivos';

// --- MIDDLEWARE ---
app.use(cors()); // Habilita CORS para permitir peticiones desde tu futuro frontend
app.use(express.json()); // Permite al servidor entender peticiones con cuerpo JSON

// --- CONEXIÓN A MONGODB ---
let db;

MongoClient.connect(MONGO_URI)
  .then(client => {
    console.log('✅ Conectado a MongoDB Atlas');
    db = client.db(DB_NAME);

    // Iniciamos el servidor solo después de conectarnos a la base de datos
    app.listen(PORT, () => {
      console.log(`🚀 API Backend corriendo en el puerto ${PORT}`);
    });
  })
  .catch(error => {
    console.error('❌ Error al conectar a MongoDB:', error);
    process.exit(1);
  });

// --- ENDPOINTS (RUTAS DE LA API) ---

// Endpoint de prueba para saber que la API está viva
app.get('/api', (req, res) => {
  res.json({ message: '¡La API de Power CRM está funcionando!' });
});

// Endpoint para obtener los datos de Jira
app.get('/api/jira/issues', async (req, res) => {
  try {
    const issues = await db.collection(JIRA_COLLECTION).find({}).toArray();
    res.status(200).json(issues);
  } catch (error) {
    console.error('Error al obtener los datos de Jira:', error);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
});

// --- ¡ACÁ PODÉS AGREGAR MÁS ENDPOINTS EN EL FUTURO! ---
// Ejemplo para rendimiento:
// app.get('/api/grafana/rendimiento', async (req, res) => { ... });