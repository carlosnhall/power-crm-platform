// services/ingestor-grafana-rendimiento/index.js (Versión Final Corregida)

const { execSync } = require('child_process');
const Papa = require('papaparse');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

// --- CONFIGURACIÓN ---
const DOWNLOAD_URL_BASE = process.env.GRAFANA_DOWNLOAD_URL;
const WINDOWS_USER = process.env.WINDOWS_USER;
const WINDOWS_PASSWORD = process.env.WINDOWS_PASSWORD;
const pool = new Pool({
    user: process.env.PG_USER,
    host: process.env.PG_HOST,
    database: process.env.PG_DATABASE,
    password: process.env.PG_PASSWORD,
    port: process.env.PG_PORT,
});
const TABLE_NAME = 'grafana_rendimiento';
const PROVEEDORES = {
    "CONNECTIS": { "id_proveedor": "11", "reportes": ['PuntualidadYReaperturasConn'] },
    "NTTDATA": { "id_proveedor": "44", "reportes": ['PuntualidadYReaperturas'] }
};

// --- FUNCIONES (sin cambios en esta sección) ---
async function downloadReport(providerConfig, monthName, monthNumber, reportName, year = "2025") {
    const k_param = `${monthNumber}_${year}`;
    console.log(`📥 Descargando reporte ${providerConfig.id_proveedor}-${reportName} para ${k_param}...`);
    if (!WINDOWS_USER || !WINDOWS_PASSWORD) {
        console.error('❌ Error: Faltan las credenciales WINDOWS_USER o WINDOWS_PASSWORD.');
        return [];
    }
    const fullUrl = `${DOWNLOAD_URL_BASE}?b=Base_INFGRF&q=AM/${reportName}&p=${providerConfig.id_proveedor}&k=${k_param}&of=download`;
    const tempDir = path.join(__dirname, 'temp_downloads');
    const tempFilePath = path.join(tempDir, `temp_${reportName}_${monthName}.csv`);
    const command = `curl --ntlm --user "${WINDOWS_USER}:${WINDOWS_PASSWORD}" --silent --fail "${fullUrl}" > "${tempFilePath}"`;
    try {
        execSync(command, { stdio: 'pipe' }); // Usamos stdio:'pipe' para suprimir la salida en caso de éxito
        const csvContent = fs.readFileSync(tempFilePath, 'utf-8');
        console.log(`[DEBUG] Archivo descargado con éxito en: ${tempFilePath}`);
        if (!csvContent || csvContent.toLowerCase().includes("html")) { return []; }
        const parsedData = Papa.parse(csvContent, { header: true, skipEmptyLines: true, delimiter: ';' });
        parsedData.data.forEach(row => {
            if (row.hasOwnProperty("")) delete row[""];
            if (row.hasOwnProperty("TRIM(A.SUBESTADO)")) {
                row.SUBESTADO = row["TRIM(A.SUBESTADO)"];
                delete row["TRIM(A.SUBESTADO)"];
            }
        });
        console.log(`✅ Se leyeron ${parsedData.data.length} filas del reporte '${reportName}'.`);
        return parsedData.data;
    } catch (error) {
        console.error(`❌ Error al ejecutar cURL para el reporte '${reportName}'.`);
        console.error("[DEBUG cURL] Mensaje de error:", error.message);
        return [];
    }
}
async function getMesesYaDescargados(providerName) {
    let client;
    try {
        client = await pool.connect();
        const query = `SELECT DISTINCT "MesConsulta" FROM ${TABLE_NAME} WHERE "Proveedor" = $1`;
        const res = await client.query(query, [providerName]);
        return res.rows.map(row => row.MesConsulta);
    } catch (error) {
        if (error.code === '42P01') { console.warn(`⚠️ La tabla ${TABLE_NAME} no existe. Se descargarán todos los meses.`); return []; }
        console.error('❌ Error consultando meses en PostgreSQL:', error);
        return [];
    } finally { if (client) { client.release(); } }
}
async function uploadToPostgres(dataToUpload) {
    if (dataToUpload.length === 0) return;
    const client = await pool.connect();
    const BATCH_SIZE = 500;
    let totalRowsAffected = 0;
    const MASTER_COLUMNS = ["Proveedor", "MesConsulta", "TipoReporte", "PROV", "ORGANIZACION", "NIVEL", "SLA", "HORARIO", "MES", "ANIO", "NUM_INCIDENTE", "FECHA_INI", "FECHA_FIN", "GRUPO", "RUTA", "PRIORIDAD", "ESTADO", "SUBESTADO", "FECHA_REAPERTURA", "IMPACTO", "MINUTOSHAB", "STATUS_ACTUAL", "FECHACREACION", "APERTURA", "CIERRE", "IDPROV"];
    const TIMESTAMP_COLUMNS = ["FECHA_INI", "FECHA_FIN", "FECHA_REAPERTURA", "FECHACREACION"];
    console.log(' -> Normalizando y limpiando datos...');
    const normalizedData = dataToUpload.map(originalRow => {
        const normalizedRow = {};
        for (const col of MASTER_COLUMNS) {
            let value = originalRow.hasOwnProperty(col) ? originalRow[col] : null;
            if (TIMESTAMP_COLUMNS.includes(col) && value === '') { value = null; }
            normalizedRow[col] = value;
        }
        return normalizedRow;
    });
    const columns = MASTER_COLUMNS.map(col => `"${col}"`);
    const conflictColumns = ['"Proveedor"', '"MesConsulta"', '"TipoReporte"', '"NUM_INCIDENTE"'];
    const onConflictUpdate = columns.filter(col => !conflictColumns.includes(col)).map(col => `${col} = EXCLUDED.${col}`).join(', ');
    console.log(` -> Enviando ${normalizedData.length} registros a la tabla ${TABLE_NAME} en lotes de ${BATCH_SIZE}...`);
    try {
        for (let i = 0; i < normalizedData.length; i += BATCH_SIZE) {
            const batch = normalizedData.slice(i, i + BATCH_SIZE);
            console.log(`   -> Procesando lote: ${i + 1} a ${i + batch.length} de ${normalizedData.length}`);
            const values = batch.map(rec => MASTER_COLUMNS.map(col => rec[col]));
            const valuePlaceholders = batch.map((_, index) => { const base = index * columns.length; return `(${columns.map((_, i) => `$${base + i + 1}`).join(', ')})`; }).join(', ');
            const query = `INSERT INTO ${TABLE_NAME} (${columns.join(', ')}) VALUES ${valuePlaceholders} ON CONFLICT (${conflictColumns.join(', ')}) DO UPDATE SET ${onConflictUpdate};`;
            const result = await client.query(query, values.flat());
            totalRowsAffected += result.rowCount;
        }
        console.log(`✅ Resultado final: ${totalRowsAffected} filas afectadas en total.`);
    } catch (error) { console.error("❌ Error al enviar datos a PostgreSQL:", error); } finally { client.release(); }
}

// --- ¡FUNCIÓN PRINCIPAL MODIFICADA! ---
async function ingestAllData() {
    console.log('--- Iniciando proceso de proveedores (Rendimiento) ---');
    
    // --- CORRECCIÓN: Limpiamos y creamos la carpeta temporal al inicio ---
    const tempDir = path.join(__dirname, 'temp_downloads');
    if (fs.existsSync(tempDir)) {
        console.log(`🧹 Limpiando directorio temporal anterior: ${tempDir}`);
        fs.rmSync(tempDir, { recursive: true, force: true });
    }
    fs.mkdirSync(tempDir, { recursive: true });
    // -----------------------------------------------------------------

    let allReportsData = [];
    for (const providerName in PROVEEDORES) {
        const providerConfig = PROVEEDORES[providerName];
        console.log(`\n--- Procesando proveedor: ${providerName} ---`);
        const mesesYaDescargados = await getMesesYaDescargados(providerName);
        const mesesDelAnio = { 'January': '01', 'February': '02', 'March': '03', 'April': '04', 'May': '05', 'June': '06', 'July': '07', 'August': '08', 'September': '09', 'October': '10', 'November': '11', 'December': '12' };
        const today = new Date();
        const currentMonthIndex = today.getMonth();
        const mesesAProcesarArray = Object.keys(mesesDelAnio).slice(0, currentMonthIndex);
        const mesesAProcesar = {};
        for (const monthName of mesesAProcesarArray) { if (!mesesYaDescargados.includes(monthName)) { mesesAProcesar[monthName] = mesesDelAnio[monthName]; } }
        if (Object.keys(mesesAProcesar).length === 0) { console.log(`✅ No hay meses nuevos para descargar para ${providerName}.`); continue; }

        for (const [monthName, number] of Object.entries(mesesAProcesar)) {
            for (const reportName of providerConfig.reportes) {
                let reportData = await downloadReport(providerConfig, monthName, number, reportName);
                
                if (reportData.length > 0) {
                    const originalCount = reportData.length;
                    if (reportName === 'PuntualidadYReaperturas') {
                        reportData = reportData.filter(row => row.GRUPO === 'N2_DEVOPS_PCRM_TEF');
                        console.log(` -> Filtrado reporte '${reportName}'. Registros pasaron de ${originalCount} a ${reportData.length}.`);
                    } else if (reportName === 'PuntualidadYReaperturasConn') {
                        reportData = reportData.filter(row => row.GRUPO === 'N1_SD_MOVIL_POWER');
                        console.log(` -> Filtrado reporte '${reportName}'. Registros pasaron de ${originalCount} a ${reportData.length}.`);
                    }

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
    }

    if (allReportsData.length > 0) {
        // ... (lógica de filtrado, deduplicación y carga no cambia) ...
        console.log(` -> Total de registros descargados (antes de filtrar): ${allReportsData.length}`);
        const dataConIncidente = allReportsData.filter(record => { return record.NUM_INCIDENTE && String(record.NUM_INCIDENTE).trim() !== ''; });
        console.log(` -> Registros después de filtrar los que no tienen NUM_INCIDENTE: ${dataConIncidente.length}`);
        const uniqueRecords = new Map();
        dataConIncidente.forEach(record => { const key = `${record.Proveedor}|${record.MesConsulta}|${record.TipoReporte}|${record.NUM_INCIDENTE}`; uniqueRecords.set(key, record); });
        const cleanData = Array.from(uniqueRecords.values());
        console.log(` -> Total de registros únicos a cargar: ${cleanData.length}`);
        await uploadToPostgres(cleanData);
    } else {
        console.log("\nℹ️ No se descargaron datos nuevos de Rendimiento en esta ejecución.");
    }
    return true;
}

ingestAllData();
module.exports = ingestAllData;