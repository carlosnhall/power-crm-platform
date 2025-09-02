// services/ingestor-grafana-rendimiento/index.js (Versión de Diagnóstico Fila por Fila)

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
    "CONNECTIS": { "id_proveedor": "11", "reportes": ['PuntualidadYReaperturasConn', 'Resumen_PendientesxnivelConn'] },
    "NTTDATA": { "id_proveedor": "44", "reportes": ['PuntualidadYReaperturas', 'PuntualidadYReaperturasPadTot', 'PuntualidadYReaperturasPadTotODS', 'Resumen_Pendientesxnivel', 'Resumen_PendientesxTotalPad', 'Resumen_PendientesxTotalPadODS', 'Ingresos_N1', 'alegaciones'] }
};

async function downloadReport(providerConfig, monthName, monthNumber, reportName, year = "2025") {
    const k_param = `${monthNumber}_${year}`;
    console.log(`📥 Descargando reporte ${providerConfig.id_proveedor}-${reportName} para ${k_param}...`);
    if (!WINDOWS_USER || !WINDOWS_PASSWORD) {
        console.error('❌ Error: Faltan las credenciales WINDOWS_USER o WINDOWS_PASSWORD.');
        return [];
    }
    const fullUrl = `${DOWNLOAD_URL_BASE}?b=Base_INFGRF&q=AM/${reportName}&p=${providerConfig.id_proveedor}&k=${k_param}&of=download`;
    const tempDir = path.join(__dirname, 'temp_downloads');
    if (!fs.existsSync(tempDir)) { fs.mkdirSync(tempDir); }
    const tempFilePath = path.join(tempDir, `temp_${reportName}_${monthName}.csv`);
    const command = `curl --ntlm --user "${WINDOWS_USER}:${WINDOWS_PASSWORD}" --silent --fail "${fullUrl}" > "${tempFilePath}"`;
    try {
        execSync(command, { stdio: 'pipe' });
        const csvContent = fs.readFileSync(tempFilePath, 'utf-8');
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
        if (error.code === '42P01') { 
            console.warn(`⚠️ La tabla ${TABLE_NAME} no existe. Se descargarán todos los meses.`);
            return [];
        }
        console.error('❌ Error consultando meses en PostgreSQL:', error);
        return [];
    } finally {
        if (client) {
            client.release();
        }
    }
}

async function uploadToPostgres(dataToUpload) {
    if (dataToUpload.length === 0) return;
    const client = await pool.connect();
    let totalRowsAffected = 0;

    const MASTER_COLUMNS = ["Proveedor", "MesConsulta", "TipoReporte", "PROV", "ORGANIZACION", "NIVEL", "SLA", "HORARIO", "MES", "ANIO", "NUM_INCIDENTE", "FECHA_INI", "FECHA_FIN", "GRUPO", "RUTA", "PRIORIDAD", "ESTADO", "SUBESTADO", "FECHA_REAPERTURA", "IMPACTO", "MINUTOSHAB", "STATUS_ACTUAL", "FECHACREACION", "APERTURA", "CIERRE", "IDPROV"];
    const TIMESTAMP_COLUMNS = ["FECHA_INI", "FECHA_FIN", "FECHA_REAPERTURA", "FECHACREACION"];

    console.log(' -> Normalizando y limpiando datos...');
    const normalizedData = dataToUpload.map(originalRow => {
        const normalizedRow = {};
        for (const col of MASTER_COLUMNS) {
            const originalKey = Object.keys(originalRow).find(k => k.trim().toUpperCase() === col.toUpperCase());
            let value = originalKey ? originalRow[originalKey] : null;
            if (TIMESTAMP_COLUMNS.includes(col) && (value === '' || value === null)) { value = null; }
            normalizedRow[col] = value;
        }
        return normalizedRow;
    });

    const columns = MASTER_COLUMNS.map(col => `"${col}"`);
    const conflictColumns = ['"Proveedor"', '"MesConsulta"', '"TipoReporte"', '"NUM_INCIDENTE"'];
    const onConflictUpdate = columns.filter(col => !conflictColumns.includes(col)).map(col => `${col} = EXCLUDED.${col}`).join(', ');

    console.log(` -> Enviando ${normalizedData.length} registros a la tabla, uno por uno para diagnóstico...`);
    try {
        for (const record of normalizedData) {
            const values = MASTER_COLUMNS.map(col => record[col]);
            const query = `INSERT INTO ${TABLE_NAME} (${columns.join(', ')}) VALUES (${columns.map((_, i) => `$${i + 1}`).join(', ')}) ON CONFLICT (${conflictColumns.join(', ')}) DO UPDATE SET ${onConflictUpdate};`;
            
            try {
                await client.query(query, values);
                totalRowsAffected++;
            } catch (rowError) {
                console.error("\n❌❌❌ HA FALLADO UNA FILA ESPECÍFICA ❌❌❌");
                console.error("Error:", rowError.message);
                console.error("Datos de la fila que causó el error:");
                console.log(record);
                for (const col of MASTER_COLUMNS) {
                    if (typeof record[col] === 'string' && record[col].length > 255) {
                        console.error(`\n---> ¡CULPABLE ENCONTRADO! La columna "${col}" tiene un valor de ${record[col].length} caracteres.`);
                        console.error(`---> Valor (primeros 300 caracteres): "${record[col].substring(0, 300)}..."`);
                    }
                }
                throw rowError; 
            }
        }
        console.log(`✅ Resultado final: ${totalRowsAffected} filas afectadas en total.`);
    } catch (error) {
        console.error("\nEl proceso de carga se detuvo debido al error en la fila anterior.");
    } finally {
        client.release();
    }
}

async function ingestAllData() {
    console.log('--- Iniciando proceso de proveedores (Rendimiento) ---');
    const tempDir = path.join(__dirname, 'temp_downloads');
    if (fs.existsSync(tempDir)) {
        fs.rmSync(tempDir, { recursive: true, force: true });
    }
    fs.mkdirSync(tempDir, { recursive: true });

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
        for (const monthName of mesesAProcesarArray) {
            if (!mesesYaDescargados.includes(monthName)) {
                mesesAProcesar[monthName] = mesesDelAnio[monthName];
            }
        }
        if (Object.keys(mesesAProcesar).length === 0) {
            console.log(`✅ No hay meses nuevos para descargar para ${providerName}.`);
            continue;
        }
        for (const [monthName, number] of Object.entries(mesesAProcesar)) {
            for (const reportName of providerConfig.reportes) {
                const reportData = await downloadReport(providerConfig, monthName, number, reportName);
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
        console.log(` -> Total de registros descargados (antes de filtrar): ${allReportsData.length}`);
        const dataConIncidente = allReportsData.filter(record => record.NUM_INCIDENTE && String(record.NUM_INCIDENTE).trim() !== '');
        console.log(` -> Registros después de filtrar los que no tienen NUM_INCIDENTE: ${dataConIncidente.length}`);
        
        const uniqueRecords = new Map();
        dataConIncidente.forEach(record => {
            const key = `${record.Proveedor}|${record.MesConsulta}|${record.TipoReporte}|${record.NUM_INCIDENTE}`;
            uniqueRecords.set(key, record);
        });
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