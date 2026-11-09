import Stack, { StackProps } from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import OrkesLogo from "images/svg/orkes-logo.svg";
import { featureFlags, FEATURES } from "utils/flags";

// Logos
const ConductorOSSLogo = "https://assets.conductor-oss.org/logo.png";
const ConductorOSSIcon = "/conductorLogoSmall.svg";

// Determine which logo to use based on ACCESS_MANAGEMENT feature flag
// Enterprise (ACCESS_MANAGEMENT enabled) uses Orkes logo
// OSS (ACCESS_MANAGEMENT disabled) uses Conductor OSS logo
const isEnterprise = featureFlags.isEnabled(FEATURES.ACCESS_MANAGEMENT);
const defaultLogo = isEnterprise ? OrkesLogo : ConductorOSSLogo;
const defaultAltText = isEnterprise ? "Orkes logo" : "Workflow Engine";

export const OpenedLogo = ({
  customLogo,
  ...rest
}: StackProps & {
  customLogo?: string;
}) => (
  <Stack
    {...rest}
    flexDirection="row"
    alignItems="center"
    justifyContent="center"
    height="100%"
    sx={
      customLogo
        ? {
            position: "relative",
            width: "100%",
            height: "100%",
            backgroundImage: `url(${customLogo})`,
            backgroundSize: "contain",
            backgroundRepeat: "no-repeat",
            backgroundPosition: "center",
            transition: "all 0.2s ease-in-out",
          }
        : {
            transition: "all 0.2s ease-in-out",
          }
    }
  >
    {customLogo ? (
      <img
        width="40px"
        src={defaultLogo}
        alt={`Powered by ${defaultAltText}`}
        style={{
          position: "absolute",
          bottom: "4px",
          right: "4px",
          opacity: 0.8,
          maxHeight: "20px",
          objectFit: "contain",
          transition: "all 0.2s ease-in-out",
        }}
      />
    ) : !isEnterprise ? (
      <Stack direction="row" alignItems="center" gap={1}>
        <img
          src={ConductorOSSIcon}
          alt=""
          aria-hidden
          style={{ height: 32, width: 32, objectFit: "contain" }}
        />
        <Typography fontWeight={600} fontSize={16} lineHeight={1.15}>
          Workflow
          <br />
          Engine
        </Typography>
      </Stack>
    ) : (
      <img
        src={defaultLogo}
        alt={defaultAltText}
        style={{
          transition: "all 0.2s ease-in-out",
          height: "100%",
          maxWidth: "80%",
          objectFit: "contain",
        }}
      />
    )}
  </Stack>
);
