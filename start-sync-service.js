#!/usr/bin/env bun

// Скрипт для запуска сервиса синхронизации Bybit с использованием Bun
const { execSync } = require('child_process');
const path = require('path');
const fs = require('fs');

// Проверка наличия Bun
try {
  execSync('bun --version', { stdio: 'ignore' });
  console.log('✅ Bun установлен');
} catch (error) {
  console.error('❌ Ошибка: Bun не установлен. Пожалуйста установите Bun: https://bun.sh');
  process.exit(1);
}

// Проверяем, установлен ли Prisma клиент
try {
  // Проверка, существует ли директория node_modules/.prisma
  if (!fs.existsSync(path.join(__dirname, 'node_modules', '.prisma'))) {
    console.log('📦 Генерация Prisma клиента...');
    execSync('bun prisma generate', { stdio: 'inherit', cwd: __dirname });
    console.log('✅ Prisma клиент сгенерирован успешно');
  }
} catch (error) {
  console.error('❌ Ошибка при генерации Prisma клиента:', error);
  process.exit(1);
}

// Запускаем сервис синхронизации напрямую с TypeScript
try {
  console.log('🚀 Запуск сервиса синхронизации Bybit...');
  // Bun может запускать TypeScript файлы напрямую, без компиляции
  execSync('bun run sync-service.ts', { stdio: 'inherit', cwd: __dirname });
} catch (error) {
  console.error('❌ Сервис синхронизации завершился с ошибкой:', error);
}
