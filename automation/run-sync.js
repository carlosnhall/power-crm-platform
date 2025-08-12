// Importar las herramientas necesarias de Node.js
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
