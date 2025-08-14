// Importar las librerías necesarias
const axios = require('axios');
const ntlm = require('axios-ntlm'); // Corregido: Importación directa
const Papa = require('papaparse');
const { MongoClient } = require('mongodb'); // Añadido para la conexión directa a la BD
require('dotenv').config();

// --- CONFIGURACIÓN ---
const DOWNLOAD_URL_BASE = process.env.GRAFANA_DOWNLOAD_URL;
const WINDOWS_USER = process.env.WINDOWS_USER;
const WINDOWS_PASSWORD = process.env.WINDOWS_PASSWORD;

// --- NUEVA CONFIGURACIÓN DE MONGODB ---
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = 'plataforma_datos_jira';
const COLLECTION_NAME = 'rendimiento'; // Nueva colección para los datos de Grafana

// --- CONFIGURACIÓN DE PROVEEDORES (sin cambios) ---
const PROVEEDORES = {
    "CONNECTIS": {
        "id_proveedor": "11",
        "grupo_filtro": "N1_SD_MOVIL_POWER",
        "reportes": ['PuntualidadYReaperturasConn', 'Resumen_PendientesxnivelConn']
    },
    "NTTDATA": {
        "id_proveedor": "44",
        "grupo_filtro": "N2_DEVOPS_PCRM_TEF",
        "reportes": [
            'PuntualidadYReaperturas', 'PuntualidadYReaperturasPadTot', 'PuntualidadYReaperturasPadTotODS',
            'Resumen_Pendientesxnivel', 'Resumen_PendientesxTotalPad', 'Resumen_PendientesxTotalPadODS',
            'Ingresos_N1', 'alegaciones'
        ]
    }
};

/**
 * Descarga y procesa un reporte específico.
 */
async function downloadReport(providerConfig, monthName, monthNumber, reportName, year = "2025") {
    // ... (Esta función no necesita cambios, está correcta)
    const k_param = `${monthNumber}_${year}`;
    console.log(`📥 Descargando reporte ${providerConfig.id_proveedor}-${reportName} para ${k_param}...`);
    const params = { 'b': 'Base_INFGRF', 'q': `AM/${reportName}`, 'p': providerConfig.id_proveedor, 'k': k_param, 'of': 'download' };
    try {
        const response = await axios({
            method: 'get',
            url: DOWNLOAD_URL_BASE,
            params,
            auth: { username: WINDOWS_USER, password: WINDOWS_PASSWORD },
            transformRequest: ntlm(),
            responseType: 'text'
        });
        const csvContent = response.data;
        if (csvContent.toLowerCase().includes("<!doctype html")) {
            console.log(`❌ Error de autenticación para el reporte '${reportName}'.`);
            return [];
        }
        const parsedData = Papa.parse(csvContent, { header: true, skipEmptyLines: true, delimiter: ';' });
        console.log(`✅ Se leyeron ${parsedData.data.length} filas del reporte '${reportName}'.`);
        return parsedData.data;
    } catch (error) {
        console.error(`❌ Error al procesar el reporte '${reportName}':`, error.message);
        return [];
    }
}

/**
 * --- FUNCIÓN REESCRITA ---
 * Consulta directamente a MongoDB para saber qué meses ya existen.
 */
async function getMesesYaDescargados(providerName) {
    if (!MONGO_URI) return []; // Si no hay URI, no podemos consultar
    const client = new MongoClient(MONGO_URI);
    try {
        await client.connect();
        const database = client.db(DB_NAME);
        const collection = database.collection(COLLECTION_NAME);
        console.log(`Consultando meses ya descargados para ${providerName} en MongoDB...`);
        // Obtenemos los valores únicos del campo 'MesConsulta' para el proveedor especificado
        const meses = await collection.distinct('MesConsulta', { Proveedor: providerName });
        return meses;
    } catch (error) {
        console.error(`Error al consultar meses en MongoDB para ${providerName}:`, error.message);
        return []; // En caso de error, devolvemos un array vacío para no bloquear el proceso
    } finally {
        await client.close();
    }
}

/**
 * --- NUEVA FUNCIÓN ---
 * Guarda los datos directamente en MongoDB.
 * La estrategia es: borrar los datos del mes y proveedor, y luego insertar los nuevos.
 */
async function uploadToMongo(dataToUpload) {
    if (dataToUpload.length === 0 || !MONGO_URI) {
        console.log("No hay datos para subir a MongoDB o falta la configuración MONGO_URI.");
        return;
    }
    const client = new MongoClient(MONGO_URI);
    try {
        await client.connect();
        const database = client.db(DB_NAME);
        const collection = database.collection(COLLECTION_NAME);
        console.log(`\n📤 Conectado a MongoDB. Enviando ${dataToUpload.length} registros a la colección '${COLLECTION_NAME}'...`);

        // Agrupamos los datos por proveedor y mes para procesarlos en lotes
        const dataByProviderAndMonth = dataToUpload.reduce((acc, row) => {
            const key = `${row.Proveedor}_${row.MesConsulta}`;
            if (!acc[key]) {
                acc[key] = { proveedor: row.Proveedor, mes: row.MesConsulta, data: [] };
            }
            acc[key].data.push(row);
            return acc;
        }, {});

        for (const key in dataByProviderAndMonth) {
            const payload = dataByProviderAndMonth[key];
            console.log(` -> Actualizando ${payload.data.length} registros de ${payload.proveedor} para ${payload.mes}...`);
            // 1. Borramos los datos viejos para este proveedor y mes
            await collection.deleteMany({ Proveedor: payload.proveedor, MesConsulta: payload.mes });
            // 2. Insertamos los datos nuevos y actualizados
            await collection.insertMany(payload.data);
        }
        console.log("✅ Datos guardados correctamente en MongoDB.");

    } catch (error) {
        console.error("❌ Error al enviar datos a MongoDB:", error.message);
    } finally {
        await client.close();
    }
}

/**
 * --- FUNCIÓN PRINCIPAL MODIFICADA ---
 * Orquesta la descarga y el guardado directo en MongoDB.
 */
async function ingestAllData() {
    let allReportsData = [];

    for (const providerName in PROVEEDORES) {
        // ... (La lógica de descarga de reportes no cambia, está correcta) ...
        const providerConfig = PROVEEDORES[providerName];
        console.log(`\n--- Iniciando proceso para el proveedor: ${providerName} ---`);
        const mesesYaDescargados = await getMesesYaDescargados(providerName);
        console.log(` -> Meses encontrados en la BD para ${providerName}:`, mesesYaDescargados);
        const mesesDelAnio = { 'January': '01', 'February': '02', 'March': '03', 'April': '04', 'May': '05', 'June': '06', 'July': '07', 'August': '08', 'September': '09', 'October': '10', 'November': '11', 'December': '12' };
        const mesActual = new Date().getMonth();
        const mesesCompletados = Object.keys(mesesDelAnio).slice(0, mesActual);
        const mesesAProcesar = {};
        for (const monthName of mesesCompletados) {
            if (!mesesYaDescargados.includes(monthName)) {
                mesesAProcesar[monthName] = mesesDelAnio[monthName];
            }
        }
        if (Object.keys(mesesAProcesar).length === 0) {
            console.log(`✅ No hay meses nuevos para descargar para ${providerName}. Todo está actualizado.`);
            continue;
        }
        console.log(` -> Meses pendientes de descarga para ${providerName}:`, Object.keys(mesesAProcesar));
        for (const [monthName, monthNumber] of Object.entries(mesesAProcesar)) {
            for (const reportName of providerConfig.reportes) {
                const reportData = await downloadReport(providerConfig, monthName, monthNumber, reportName);
                if (reportData.length > 0) {
                    reportData.forEach(row => {
                        row.Proveedor = providerName;
                        row.MesConsulta = monthName;
                        row.TipoReporte = reportName;
                    });
                    allReportsData.push(...reportData);
                }
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    }

    if (allReportsData.length === 0) {
        console.log("\nℹ️ No se descargaron datos nuevos en esta ejecución.");
        return;
    }

    console.log(`\n✅ Se consolidaron un total de ${allReportsData.length} filas antes de filtrar.`);
    
    const filteredData = allReportsData.filter(row => {
        const grupo = row.GRUPO || row.grupo;
        if (!grupo) return false;
        const grupoUpper = String(grupo).toUpperCase();
        const connectisMatch = (row.Proveedor === 'CONNECTIS' && grupoUpper === 'N1_SD_MOVIL_POWER');
        const nttdataMatch = (row.Proveedor === 'NTTDATA' && grupoUpper === 'N2_DEVOPS_PCRM_TEF');
        return connectisMatch || nttdataMatch;
    });

    console.log(`✅ Después del filtro, quedan ${filteredData.length} filas.`);
    
    // --- LÓGICA DE GUARDADO MODIFICADA ---
    // Ahora llamamos a la función que guarda directamente en MongoDB
    if (filteredData.length > 0) {
        await uploadToMongo(filteredData);
    }
}

// Exportamos la función principal para que el orquestador pueda llamarla.
module.exports = ingestAllData;