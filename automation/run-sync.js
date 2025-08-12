// Importar las herramientas necesarias de Node.js
const { exec } = require('child_process');
const path = require('path');

// --- FUNCIÓN PARA EJECUTAR UN COMANDO ---
function runCommand(command, cwd) {
    return new Promise((resolve, reject) => {
        console.log(`Ejecutando comando: ${command} en ${cwd}`);
        const process = exec(command, { cwd });

        process.stdout.on('data', (data) => {
            console.log(`[${path.basename(cwd)}]: ${data.toString().trim()}`);
        });

        process.stderr.on('data', (data) => {
            console.error(`[${path.basename(cwd)} ERROR]: ${data.toString().trim()}`);
        });

        process.on('close', (code) => {
            if (code === 0) {
                resolve();
            } else {
                reject(new Error(`El comando "${command}" falló con el código ${code}`));
            }
        });
    });
}

// --- SCRIPT PRINCIPAL DE ORQUESTACIÓN ---
async function main() {
    console.log("🚀 Iniciando el proceso de sincronización automatizada...");

    const persistenceApiPath = path.join(__dirname, '..', 'services', 'persistence-api');
    const ingestorPath = path.join(__dirname, '..', 'services', 'ingestor-grafana-rendimiento');

    let persistenceProcess;
    let ingestorProcess;

    try {
        // 1. Instalar dependencias para ambos servicios
        console.log("\n--- Instalando dependencias del servicio de persistencia ---");
        await runCommand('npm install', persistenceApiPath);

        console.log("\n--- Instalando dependencias del servicio de ingesta ---");
        await runCommand('npm install', ingestorPath);

        // 2. Iniciar ambos servicios en segundo plano
        console.log("\n--- Iniciando microservicios en segundo plano ---");
        persistenceProcess = exec('npm start', { cwd: persistenceApiPath });
        ingestorProcess = exec('npm start', { cwd: ingestorPath });

        // Darle tiempo a los servicios para que inicien
        console.log("Esperando 15 segundos para que los servicios se inicien...");
        await new Promise(resolve => setTimeout(resolve, 15000));

        // 3. Disparar el proceso de ingesta
        console.log("\n--- Disparando la ingesta de datos ---");
        await runCommand('curl -X POST http://localhost:3002/trigger-ingest', '.');

        console.log("\n✅ Proceso de ingesta disparado con éxito.");

    } catch (error) {
        console.error("\n❌ Ocurrió un error durante la orquestación:", error);
    } finally {
        // 4. Detener los servicios para que la automatización pueda terminar
        console.log("\n--- Deteniendo los microservicios ---");
        if (persistenceProcess) persistenceProcess.kill();
        if (ingestorProcess) ingestorProcess.kill();
        console.log("🏁 Sincronización finalizada.");
    }
}

main();
