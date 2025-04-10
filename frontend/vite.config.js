import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  css: {
    postcss: './postcss.config.js',
  },
  server: {
    host: '0.0.0.0',  // Ensure it binds to all network interfaces in Docker
    port: 8080,        // Ensure it matches your Docker port
    strictPort: true,
    watch: {
      usePolling: true, // Fixes issues with WSL/docker file watching
    },
  },
});