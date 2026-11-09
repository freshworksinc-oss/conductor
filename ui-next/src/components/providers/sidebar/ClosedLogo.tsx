import OrkesIcon from "images/svg/orkes-icon.svg";
import { featureFlags, FEATURES } from "utils/flags";

// Logos
const ConductorOSSLogo = "https://assets.conductor-oss.org/logo.png";
const ConductorOSSIcon = "/conductorLogoSmall.svg";

// Determine which logo to use based on ACCESS_MANAGEMENT feature flag
// Enterprise (ACCESS_MANAGEMENT enabled) uses Orkes icon
// OSS (ACCESS_MANAGEMENT disabled) uses Conductor OSS logo
const isEnterprise = featureFlags.isEnabled(FEATURES.ACCESS_MANAGEMENT);
const defaultLogo = isEnterprise ? OrkesIcon : ConductorOSSIcon;
const defaultAltText = isEnterprise ? "Orkes logo" : "Workflow Engine";

export const ClosedLogo = ({ customLogo }: { customLogo?: string }) => (
  <img
    src={customLogo || defaultLogo}
    alt={defaultAltText}
    style={{
      width: "32px",
      height: "32px",
      objectFit: "contain",
    }}
  />
);
