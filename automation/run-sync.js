// automation/run-sync.js

// Importamos las funciones principales de nuestros ingestores.
// ¡Asegúrate de que estas rutas apunten a los archivos correctos de tus servicios!
const ingestJiraData = require('../services/ingestor-jira/api-ingestor.js'); 
const ingestGrafanaData = require('../services/ingestor-grafana-rendimiento/index.js'); 

/**
 * Función principal que orquesta la ejecución de los ingestores.
 */
async function runSynchronization() {
  console.log('--- Iniciando Sincronización de Datos ---');

  try {
    // Ejecutamos el ingestor de Jira y esperamos a que termine
    console.log('\n--- Ejecutando Ingestor de Jira ---');
    await ingestJiraData();
    console.log('--- Ingestor de Jira finalizado ---');

    // Ejecutamos el ingestor de Grafana y esperamos a que termine
    console.log('\n--- Ejecutando Ingestor de Grafana ---');
    await ingestGrafanaData();
    console.log('--- Ingestor de Grafana finalizado ---');

    console.log('\n✅ Sincronización de todos los datos completada con éxito.');

  } catch (error) {
    console.error('❌ Ocurrió un error grave durante la orquestación:', error);
    // process.exit(1) le dice a GitHub Actions que el job falló
    process.exit(1);
  }
}

// Ejecutamos la orquestación
runSynchronization();