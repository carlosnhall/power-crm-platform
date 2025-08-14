module.exports = {
  env: {
    commonjs: true, // Habilita el reconocimiento de 'require' y 'module.exports'
    es2021: true,
    node: true,     // Habilita las variables globales de Node.js
  },
  extends: 'eslint:recommended',
  parserOptions: {
    ecmaVersion: 'latest',
  },
  rules: {
    // Aquí podés agregar reglas personalizadas en el futuro si querés
  },
};