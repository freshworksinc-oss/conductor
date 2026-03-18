const { createProxyMiddleware } = require("http-proxy-middleware");
const target = process.env.WF_SERVER || "http://localhost:8080";
const usermanagementTarget =
  process.env.USERMANAGEMENT_SERVER ||
  "http://usermanagement.conductor-sandbox.svc.cluster.local:9092";

module.exports = function (app) {
  app.use(
    "/api",
    createProxyMiddleware({
      target: target,
      //pathRewrite: { "^/api/": "/" },
      changeOrigin: true,
    })
  );
  app.use(
    "/health",
    createProxyMiddleware({
      target: target,
      changeOrigin: true,
    })
  );
  app.use(
    "/usermanagement",
    createProxyMiddleware({
      target: usermanagementTarget,
      pathRewrite: { "^/usermanagement": "" },
      changeOrigin: true,
    })
  );
};
