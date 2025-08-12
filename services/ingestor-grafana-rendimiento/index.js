// Importar las librerías necesarias
const express = require('express');
const axios = require('axios');
const { ntlm } = require('axios-ntlm'); // Importación corregida
const Papa = require('papaparse');
require('dotenv').config();

// --- CONFIGURACIÓN ---
const app = express();
const PORT = process.env.PORT || 3002;

const DOWNLOAD_URL_BASE = process.env.GRAFANA_DOWNLOAD_URL;
const WINDOWS_USER = process.env.WINDOWS_USER;
const WINDOWS_PASSWORD = process.env.WINDOWS_PASSWORD;
const PERSISTENCE_API_URL = process.env.PERSISTENCE_API_URL;

// --- CONFIGURACIÓN DE PROVEEDORES ---
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
    const k_param = `${monthNumber}_${year}`;
    console.log(`📥 Descargando reporte ${providerConfig.id_proveedor}-${reportName} para ${k_param}...`);

    const params = {
        'b': 'Base_INFGRF',
        'q': `AM/${reportName}`,
        'p': providerConfig.id_proveedor,
        'k': k_param,
        'of': 'download'
    };

    try {
        // --- CORRECCIÓN: Uso correcto de axios-ntlm ---
        const response = await axios({
            method: 'get',
            url: DOWNLOAD_URL_BASE,
            params,
            auth: {
                username: WINDOWS_USER,
                password: WINDOWS_PASSWORD
            },
            transformRequest: ntlm(), // Aplicar el transformador NTLM
            responseType: 'text'
        });
        // ---------------------------------------------

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
 * Pregunta a la API de persistencia qué meses ya existen en la base de datos.
 */
async function getMesesYaDescargados(providerName) {
    try {
        console.log(`Consultando meses ya descargados para ${providerName}...`);
        const response = await axios.get(`${PERSISTENCE_API_URL}/api/rendimiento/${providerName}/meses`);
        return response.data; // Debería ser un array como ['January', 'February']
    } catch (error) {
        console.error(`Error al consultar meses descargados para ${providerName}:`, error.message);
        return [];
    }
}

/**
 * Orquesta la descarga de todos los reportes.
 */
async function ingestAllData() {
    let allReportsData = [];

    for (const providerName in PROVEEDORES) {
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
        return { success: true, count: 0 };
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
    
    if (filteredData.length > 0) {
        console.log(`\n📤 Enviando ${filteredData.length} registros al servicio de persistencia...`);
        try {
            const dataByProviderAndMonth = filteredData.reduce((acc, row) => {
                const key = `${row.Proveedor}_${row.MesConsulta}`;
                if (!acc[key]) {
                    acc[key] = { proveedor: row.Proveedor, mes: row.MesConsulta, data: [] };
                }
                acc[key].data.push(row);
                return acc;
            }, {});

            for (const key in dataByProviderAndMonth) {
                const payload = dataByProviderAndMonth[key];
                console.log(` -> Enviando ${payload.data.length} registros de ${payload.proveedor} para ${payload.mes}...`);
                await axios.post(`${PERSISTENCE_API_URL}/api/rendimiento`, payload);
            }
            console.log("✅ Datos enviados correctamente a la API de persistencia.");
            return { success: true, count: filteredData.length };

        } catch (error) {
            console.error("❌ Error al enviar datos al servicio de persistencia:", error.response ? error.response.data : error.message);
            return { success: false, count: 0 };
        }
    }
    return { success: true, count: 0 };
}


// --- API ENDPOINT ---
app.post('/trigger-ingest', async (req, res) => {
    try {
        const result = await ingestAllData();
        if (result.success) {
            res.status(200).json({ message: `Ingesta y guardado completados. Se procesaron ${result.count} filas.` });
        } else {
            res.status(500).json({ message: "La ingesta de datos funcionó, pero falló el guardado en la base de datos." });
        }
    } catch (error) {
        console.error("Error crítico en el endpoint /trigger-ingest:", error);
        res.status(500).json({ message: "Ocurrió un error crítico en el proceso de ingesta." });
    }
});

app.listen(PORT, () => {
    console.log(`🚀 Microservicio 'ingestor-grafana-rendimiento' corriendo en el puerto ${PORT}`);
});
