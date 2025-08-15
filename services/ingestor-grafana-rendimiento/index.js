// Importar las librerías necesarias
const { execSync } = require('child_process');
const Papa = require('papaparse');
const { MongoClient } = require('mongodb');
require('dotenv').config();

// --- CONFIGURACIÓN ---
const DOWNLOAD_URL_BASE = process.env.GRAFANA_DOWNLOAD_URL;
const WINDOWS_USER = process.env.WINDOWS_USER;
const WINDOWS_PASSWORD = process.env.WINDOWS_PASSWORD;

const MONGO_URI = process.env.MONGO_URI;
// --- ¡AQUÍ ESTÁ LA CLAVE! ---
// Usamos nombres fijos para la base de datos y la colección.
const DB_NAME = 'power_crm_data';
const COLLECTION_NAME = 'grafana_rendimiento'; 

const PROVEEDORES = {
    "CONNECTIS": {
        "id_proveedor": "11", "reportes": ['PuntualidadYReaperturasConn', 'Resumen_PendientesxnivelConn']
    },
    "NTTDATA": {
        "id_proveedor": "44", "reportes": ['PuntualidadYReaperturas', 'PuntualidadYReaperturasPadTot', 'PuntualidadYReaperturasPadTotODS', 'Resumen_Pendientesxnivel', 'Resumen_PendientesxTotalPad', 'Resumen_PendientesxTotalPadODS', 'Ingresos_N1', 'alegaciones']
    }
};

async function downloadReport(providerConfig, monthName, monthNumber, reportName, year = "2025") {
    const k_param = `${monthNumber}_${year}`;
    console.log(`📥 Descargando reporte ${providerConfig.id_proveedor}-${reportName} para ${k_param} usando cURL...`);
    if (!WINDOWS_USER || !WINDOWS_PASSWORD) {
        console.error('❌ Error: Faltan las credenciales WINDOWS_USER o WINDOWS_PASSWORD en los secretos.');
        return [];
    }
    const fullUrl = `${DOWNLOAD_URL_BASE}?b=Base_INFGRF&q=AM/${reportName}&p=${providerConfig.id_proveedor}&k=${k_param}&of=download`;
    const command = `curl --ntlm --user "${WINDOWS_USER}:${WINDOWS_PASSWORD}" --silent --fail "${fullUrl}"`;
    try {
        const csvContent = execSync(command, { encoding: 'utf-8' });
        if (!csvContent || csvContent.toLowerCase().includes("html")) { return []; }
        const parsedData = Papa.parse(csvContent, { header: true, skipEmptyLines: true, delimiter: ';' });
        console.log(`✅ Se leyeron ${parsedData.data.length} filas del reporte '${reportName}'.`);
        return parsedData.data;
    } catch (error) {
        console.error(`❌ Error al ejecutar cURL para el reporte '${reportName}'. Verifica la URL y las credenciales de Windows.`);
        return [];
    }
}

async function getMesesYaDescargados(providerName) {
    if (!MONGO_URI) return [];
    const client = new MongoClient(MONGO_URI);
    try {
        await client.connect();
        const database = client.db(DB_NAME);
        const collection = database.collection(COLLECTION_NAME);
        const meses = await collection.distinct('MesConsulta', { Proveedor: providerName });
        return meses;
    } catch (error) { return []; } finally { await client.close(); }
}

async function uploadToMongo(dataToUpload) {
    if (dataToUpload.length === 0 || !MONGO_URI) return;
    const client = new MongoClient(MONGO_URI);
    try {
        await client.connect();
        const database = client.db(DB_NAME);
        const collection = database.collection(COLLECTION_NAME);
        const dataByProviderAndMonth = dataToUpload.reduce((acc, row) => {
            const key = `${row.Proveedor}_${row.MesConsulta}`;
            if (!acc[key]) { acc[key] = { proveedor: row.Proveedor, mes: row.MesConsulta, data: [] }; }
            acc[key].data.push(row);
            return acc;
        }, {});
        for (const key in dataByProviderAndMonth) {
            const payload = dataByProviderAndMonth[key];
            console.log(` -> Actualizando ${payload.data.length} registros de ${payload.proveedor} para ${payload.mes}...`);
            await collection.deleteMany({ Proveedor: payload.proveedor, MesConsulta: payload.mes });
            await collection.insertMany(payload.data);
        }
    } catch (error) { console.error("❌ Error al enviar datos a MongoDB:", error.message); } finally { await client.close(); }
}

async function ingestAllData() {
    console.log('--- Iniciando proceso para proveedores de Grafana ---');
    let allReportsData = [];
    for (const providerName in PROVEEDORES) {
        const providerConfig = PROVEEDORES[providerName];
        console.log(`\n--- Procesando proveedor: ${providerName} ---`);
        const mesesYaDescargados = await getMesesYaDescargados(providerName);
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
            console.log(`✅ No hay meses nuevos para descargar para ${providerName}.`);
            continue;
        }
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
            }
        }
    }
    if (allReportsData.length > 0) {
        await uploadToMongo(allReportsData);
    } else {
        console.log("\nℹ️ No se descargaron datos nuevos de Grafana en esta ejecución.");
    }
    return true;
}

module.exports = ingestAllData;