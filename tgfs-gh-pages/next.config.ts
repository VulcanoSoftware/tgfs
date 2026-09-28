import type { NextConfig } from "next";

const basePath = process.env.NEXT_PUBLIC_BASE_PATH ?? "/tgfs";

const nextConfig: NextConfig = {
  output: "export",
  trailingSlash: true,
  basePath: basePath,
  assetPrefix: basePath || undefined,
  images: {
    loader: "custom",
    loaderFile: "image-loader.js",
  },
  allowedDevOrigins: process.env.BASE44_PUBLIC_HOST_SUFFIX
    ? [`3000-${process.env.BASE44_PUBLIC_HOST_SUFFIX}`]
    : [],
  env: {
    NEXT_PUBLIC_BASE_PATH: basePath,
  },
};

export default nextConfig;
