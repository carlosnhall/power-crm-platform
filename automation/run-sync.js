# Nombre del flujo de trabajo que verás en GitHub
name: Sincronización Diaria de Datos

# ¿Cuándo se debe ejecutar este flujo?
on:
  # Permite ejecutarlo manualmente desde la pestaña "Actions" de GitHub
  workflow_dispatch:

  # Se ejecuta automáticamente todos los días a las 11:00 UTC (8:00 AM en Argentina)
  schedule:
    - cron: '0 11 * * *'

# ¿Qué trabajos se deben realizar?
jobs:
  sync-data:
    # El tipo de máquina virtual en la que se ejecutará
    runs-on: ubuntu-latest

    # Los pasos que se ejecutarán en orden
    steps:
      # 1. Descarga tu código del repositorio a la máquina virtual
      - name: Checkout del código
        uses: actions/checkout@v3

      # 2. Configura Node.js para que podamos usarlo
      - name: Setup Node.js
        uses: actions/setup-node@v3
        with:
          node-version: '18' # Usamos la versión 18 de Node.js

      # 3. Crear los archivos .env a partir de los "secretos" de GitHub
      # ¡Este paso es crucial para la seguridad!
      - name: Crear .env para el servicio de persistencia
        run: |
          echo "PORT=3003" >> ./services/persistence-api/.env
          echo "MONGO_URI=${{ secrets.MONGO_URI }}" >> ./services/persistence-api/.env
      
      - name: Crear .env para el servicio de ingesta de rendimiento
        run: |
          echo "PORT=3002" >> ./services/ingestor-grafana-rendimiento/.env
          echo "GRAFANA_DOWNLOAD_URL=${{ secrets.GRAFANA_DOWNLOAD_URL }}" >> ./services/ingestor-grafana-rendimiento/.env
          echo "WINDOWS_USER=${{ secrets.WINDOWS_USER }}" >> ./services/ingestor-grafana-rendimiento/.env
          echo "WINDOWS_PASSWORD=${{ secrets.WINDOWS_PASSWORD }}" >> ./services/ingestor-grafana-rendimiento/.env
          echo "PERSISTENCE_API_URL=http://localhost:3003" >> ./services/ingestor-grafana-rendimiento/.env
      
      # Crear el .env para el servicio de Jira
      - name: Crear .env para el servicio de ingesta de Jira
        run: |
          echo "PORT=3004" >> ./services/ingestor-jira/.env
          echo "JIRA_BASE_URL=${{ secrets.JIRA_BASE_URL }}" >> ./services/ingestor-jira/.env
          echo "JIRA_USER_EMAIL=${{ secrets.JIRA_USER_EMAIL }}" >> ./services/ingestor-jira/.env
          echo "JIRA_API_TOKEN=${{ secrets.JIRA_API_TOKEN }}" >> ./services/ingestor-jira/.env
          echo "JIRA_PROJECT_KEYS=${{ secrets.JIRA_PROJECT_KEYS }}" >> ./services/ingestor-jira/.env
          echo "PERSISTENCE_API_URL=http://localhost:3003" >> ./services/ingestor-jira/.env

      # 4. Ejecutar el script orquestador que creamos
      - name: Ejecutar el script de sincronización
        run: node ./automation/run-sync.js// Importar las herramientas necesarias de Node.js
const { exec } = require('child_process');
const path = require('path');

// --- FUNCIÓN PARA EJECUTAR UN COMANDO ---
function runCommand(command, cwd) {
    return new Promise((resolve, reject) => {
        console.log(`Ejecutando comando: ${command} en ${cwd}`);
        const process = exec(command, { cwd });

        process.stdout.on('data', (data) => console.log(`[${path.basename(cwd)}]: ${data.toString().trim()}`));
        process.stderr.on('data', (data) => console.error(`[${path.basename(cwd)} ERROR]: ${data.toString().trim()}`));
        process.on('close', (code) => {
            if (code === 0) resolve();
            else reject(new Error(`El comando "${command}" falló con el código ${code}`));
        });
    });
}

// --- SCRIPT PRINCIPAL DE ORQUESTACIÓN ---
async function main() {
    console.log("🚀 Iniciando el proceso de sincronización automatizada...");

    // Definimos las rutas a todos nuestros servicios
    const persistenceApiPath = path.join(__dirname, '..', 'services', 'persistence-api');
    const ingestorRendimientoPath = path.join(__dirname, '..', 'services', 'ingestor-grafana-rendimiento');
    const ingestorJiraPath = path.join(__dirname, '..', 'services', 'ingestor-jira'); // <-- NUEVO

    let persistenceProcess, ingestorRendimientoProcess, ingestorJiraProcess;

    try {
        // 1. Instalar dependencias para todos los servicios
        console.log("\n--- Instalando dependencias ---");
        await runCommand('npm install', persistenceApiPath);
        await runCommand('npm install', ingestorRendimientoPath);
        await runCommand('npm install', ingestorJiraPath); // <-- NUEVO

        // 2. Iniciar todos los servicios en segundo plano
        console.log("\n--- Iniciando microservicios en segundo plano ---");
        persistenceProcess = exec('npm start', { cwd: persistenceApiPath });
        ingestorRendimientoProcess = exec('npm start', { cwd: ingestorRendimientoPath });
        ingestorJiraProcess = exec('npm start', { cwd: ingestorJiraPath }); // <-- NUEVO

        console.log("Esperando 20 segundos para que los servicios se inicien...");
        await new Promise(resolve => setTimeout(resolve, 20000));

        // 3. Disparar el proceso de ingesta para CADA servicio
        console.log("\n--- Disparando la ingesta de datos ---");
        await runCommand('curl -X POST http://localhost:3002/trigger-ingest', '.'); // Rendimiento
        await runCommand('curl -X POST http://localhost:3004/trigger-ingest', '.'); // Jira <-- NUEVO
        
        console.log("\n✅ Procesos de ingesta disparados con éxito.");

    } catch (error) {
        console.error("\n❌ Ocurrió un error durante la orquestación:", error);
    } finally {
        // 4. Detener todos los servicios
        console.log("\n--- Deteniendo los microservicios ---");
        if (persistenceProcess) persistenceProcess.kill();
        if (ingestorRendimientoProcess) ingestorRendimientoProcess.kill();
        if (ingestorJiraProcess) ingestorJiraProcess.kill(); // <-- NUEVO
        console.log("🏁 Sincronización finalizada.");
    }
}

main();

