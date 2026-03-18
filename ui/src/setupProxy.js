const { createProxyMiddleware } = require("http-proxy-middleware");
const target = process.env.WF_SERVER || "http://localhost:8080";
const usermanagementTarget =
  process.env.USERMANAGEMENT_SERVER ||
  "http://usermanagement.conductor-sandbox.svc.cluster.local:9092";

module.exports = function (app) {
  // Server-side endpoint: returns user email and tenants in one call
  app.get("/userinfo", async (req, res) => {
    console.log("[userinfo] Request received");
    const userEmail = req.headers["x-user-email"];
    console.log("[userinfo] x-user-email header:", userEmail || "NOT FOUND");

    if (!userEmail) {
      console.log("[userinfo] Returning 401 - no email header");
      return res.status(401).json({ error: "x-user-email header not found" });
    }

    try {
      const url = `${usermanagementTarget}/v2/auth/${encodeURIComponent(
        userEmail
      )}/conductor?details=true`;
      console.log("[userinfo] Calling usermanagement:", url);
      const response = await fetch(url);
      console.log("[userinfo] Usermanagement response status:", response.status);

      if (!response.ok) {
        const errorText = await response.text();
        console.error("[userinfo] Usermanagement error response:", errorText);
        return res
          .status(response.status)
          .json({ error: `Usermanagement returned ${response.status}` });
      }
      const data = await response.json();
      console.log("[userinfo] Tenants data:", JSON.stringify(data));
      res.json({ email: userEmail, tenants: data.message || {} });
    } catch (err) {
      console.error("[userinfo] Failed to fetch tenants:", err.message);
      res.status(500).json({ error: "Failed to fetch tenants" });
    }
  });

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
};
