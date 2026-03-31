import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": "http://localhost:9700",
      "/health": "http://localhost:9700",
      "/metrics": "http://localhost:9700",
    },
  },
});
