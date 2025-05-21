#!/usr/bin/env node
/**
 * Automated scheduler for Google Ads Customer Data Upload Tool
 * 
 * This script provides a simple way to schedule daily/weekly uploads for all brands
 * or specific brands at defined intervals.
 */

const { scheduleJob } = require('node-schedule');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const winston = require('winston');

// Initialize logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ level, message, timestamp }) => {
      return `${timestamp} - ${level.toUpperCase()}: ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'scheduler.log' })
  ]
});

// Configuration - can be modified to fit your needs
const config = {
  // Default schedule (daily at 2:00 AM)
  defaultSchedule: '0 2 * * *',
  
  // Path to the Google Ads uploader script
  uploaderPath: path.join(__dirname, 'googleAdsUploader.js'),
  
  // Default arguments for the uploader
  defaultArgs: ['--all-brands', '--silent'],
  
  // Brand-specific schedules (optional)
  brandSchedules: [
    // Examples - uncomment and customize as needed
    // { brand: 'brandname1', schedule: '0 1 * * *', mode: 'delta' },  // 1:00 AM daily
    // { brand: 'brandname2', schedule: '0 3 * * 1', mode: 'full' },   // 3:00 AM on Mondays
  ]
};

// Check if the uploader script exists
if (!fs.existsSync(config.uploaderPath)) {
  logger.error(`Uploader script not found at: ${config.uploaderPath}`);
  process.exit(1);
}

// Schedule the default job (all brands)
logger.info(`Scheduling default job (all brands) with schedule: ${config.defaultSchedule}`);
scheduleJob('default-all-brands', config.defaultSchedule, () => {
  const timestamp = new Date().toISOString();
  logger.info(`[${timestamp}] Running scheduled job for all brands`);
  
  const args = [...config.defaultArgs];
  runUploaderProcess(args);
});

// Schedule brand-specific jobs if configured
if (config.brandSchedules && config.brandSchedules.length > 0) {
  config.brandSchedules.forEach((brandConfig, index) => {
    const { brand, schedule, mode } = brandConfig;
    logger.info(`Scheduling job for brand '${brand}' with schedule: ${schedule}`);
    
    scheduleJob(`brand-${index}-${brand}`, schedule, () => {
      const timestamp = new Date().toISOString();
      logger.info(`[${timestamp}] Running scheduled job for brand '${brand}'`);
      
      const args = ['--brand', brand, '--silent'];
      if (mode) {
        args.push('--mode', mode);
      }
      
      runUploaderProcess(args);
    });
  });
}

/**
 * Run the uploader process with the specified arguments
 */
function runUploaderProcess(args) {
  const process = spawn('node', [config.uploaderPath, ...args], {
    env: { ...process.env, NODE_ENV: 'production' }
  });
  
  process.stdout.on('data', (data) => {
    logger.info(`[Uploader] ${data.toString().trim()}`);
  });
  
  process.stderr.on('data', (data) => {
    logger.error(`[Uploader] ${data.toString().trim()}`);
  });
  
  process.on('close', (code) => {
    if (code === 0) {
      logger.info(`Uploader process completed successfully with code ${code}`);
    } else {
      logger.error(`Uploader process failed with code ${code}`);
    }
  });
}

logger.info('Google Ads Upload Scheduler started');
logger.info('Press Ctrl+C to exit');

// Keep the process running
process.stdin.resume();

// Handle graceful shutdown
process.on('SIGINT', () => {
  logger.info('Scheduler shutting down');
  process.exit(0);
});