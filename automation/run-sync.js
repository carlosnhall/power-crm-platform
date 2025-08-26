// automation/run-sync.js

// 1. Importamos la función principal del ingestor de Jira.
const ingestJiraData = require('../services/ingestor-jira/api-ingestor.js'); 
const ingestIndividuosData = require('../services/ingestor-grafana-individuos/index.js');
const ingestMasivosData = require('../services/ingestor-grafana-masivos/index.js');
// --- Los ingestores de Grafana quedan comentados para el futuro ---
// const ingestGrafanaRendimiento = require('../services/ingestor-grafana-rendimiento/index.js'); 



/**
 * Función principal que orquesta la ejecución de los ingestores en orden.
 */
async function runSynchronization() {
  console.log('--- Iniciando Sincronización de Datos ---');

  try {
    // --- Ejecución del Ingestor de Jira ---
    console.log('\n--- Ejecutando Ingestor de Jira ---');
    await ingestJiraData();
    console.log('--- Ingestor de Jira finalizado ---');
    
    // --- Las llamadas a los ingestores de Grafana quedan comentadas ---

    console.log('\n--- Ejecutando Ingestor de Grafana (Individuos) ---');
    await ingestIndividuosData();
    console.log('--- Ingestor de Grafana (Individuos) finalizado ---');

    console.log('\n--- Ejecutando Ingestor de Grafana (Masivos) ---');
    await ingestMasivosData();
    console.log('--- Ingestor de Grafana (Masivos) finalizado ---');

    /*
    console.log('\n--- Ejecutando Ingestor de Grafana (Rendimiento) ---');
    await ingestGrafanaRendimiento();
    console.log('--- Ingestor de Grafana (Rendimiento) finalizado ---');


    */

    // --- Mensaje Final ---
    console.log('\n✅ Sincronización de Jira completada con éxito.');

  } catch (error) {
    console.error('❌ Ocurrió un error grave durante la orquestación:', error);
    // process.exit(1) le dice a GitHub Actions que el job falló
    process.exit(1);
  }
}

// Ejecutamos la orquestación
runSynchronization();