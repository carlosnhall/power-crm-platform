// automation/run-sync.js

// Importamos las funciones principales de nuestros ingestores.
const ingestJiraData = require('../services/ingestor-jira/api-ingestor.js'); 
const ingestGrafanaData = require('../services/ingestor-grafana-rendimiento/index.js'); 
// --- ¡AQUÍ ESTÁ LA CORRECCIÓN! ---
const ingestIndividuosData = require('../services/ingestor-grafana-individuos/index.js'); 

/**
 * Función principal que orquesta la ejecución de los ingestores.
 */
async function runSynchronization() {
  console.log('--- Iniciando Sincronización de Datos ---');
  try {
    console.log('\n--- Ejecutando Ingestor de Jira ---');
    await ingestJiraData();
    console.log('--- Ingestor de Jira finalizado ---');
    
    console.log('\n--- Ejecutando Ingestor de Grafana (Rendimiento) ---');
    await ingestGrafanaData();
    console.log('--- Ingestor de Grafana (Rendimiento) finalizado ---');

    console.log('\n--- Ejecutando Ingestor de Grafana (Individuos) ---');
    await ingestIndividuosData();
    console.log('--- Ingestor de Grafana (Individuos) finalizado ---');

    console.log('\n✅ Sincronización de todos los datos completada con éxito.');

  } catch (error) {
    console.error('❌ Ocurrió un error grave durante la orquestación:', error);
    process.exit(1);
  }
}

// Ejecutamos la orquestación
runSynchronization();