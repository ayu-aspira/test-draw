import tsconfigPaths from "vite-tsconfig-paths";
import { defineConfig } from "vitest/config";
import { config } from "./../../vitest.base";

// biome-ignore lint/style/noDefaultExport: vitest config
export default defineConfig({
  ...config,
  plugins: [tsconfigPaths()],
});
