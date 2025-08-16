// automation/run-sync.js

// 1. Importamos las funciones principales de todos nuestros ingestores.
// ¡Asegúrate de que estas rutas apunten a los archivos correctos de tus servicios!
const ingestJiraData = require('../services/ingestor-jira/api-ingestor.js'); 
const ingestGrafanaRendimiento = require('../services/ingestor-grafana-rendimiento/index.js'); 
const ingestIndividuosData = require('../services/ingestor-grafana-individuos/index.js');
const ingestMasivosData = require('../services/ingestor-grafana-masivos/index.js');

/**
 * Función principal que orquesta la ejecución de los ingestores en orden.
 */
async function runSynchronization() {
  console.log('--- Iniciando Sincronización de Datos ---');

  try {
    // --- Ejecución de Ingestores ---
    console.log('\n--- Ejecutando Ingestor de Jira ---');
    await ingestJiraData();
    console.log('--- Ingestor de Jira finalizado ---');
    
    console.log('\n--- Ejecutando Ingestor de Grafana (Rendimiento) ---');
    await ingestGrafanaRendimiento();
    console.log('--- Ingestor de Grafana (Rendimiento) finalizado ---');

    console.log('\n--- Ejecutando Ingestor de Grafana (Individuos) ---');
    await ingestIndividuosData();
    console.log('--- Ingestor de Grafana (Individuos) finalizado ---');

    console.log('\n--- Ejecutando Ingestor de Grafana (Masivos) ---');
    await ingestMasivosData();
    console.log('--- Ingestor de Grafana (Masivos) finalizado ---');

    // --- Mensaje Final ---
    console.log('\n✅ Sincronización de todos los datos completada con éxito.');

  } catch (error) {
    console.error('❌ Ocurrió un error grave durante la orquestación:', error);
    // process.exit(1) le dice a GitHub Actions que el job falló
    process.exit(1);
  }
}

// Ejecutamos la orquestación
runSynchronization();