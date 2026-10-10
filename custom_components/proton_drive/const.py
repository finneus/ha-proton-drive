"""Constants for the Proton Drive integration."""

from logging import Logger, getLogger

LOGGER: Logger = getLogger(__package__)

DOMAIN = "proton_drive"

CONF_BACKUP_FOLDER = "backup_folder"

CLI_VERSION = "0.9.0"

CLI_BASE_URL_FORMAT: dict[str, str] = {
    "glibc": "https://proton.me/download/drive/cli/{version}/linux-{arch}/proton-drive",
    "musl": "https://proton.me/download/drive/cli/{version}/linux-{arch}-musl/proton-drive",
}
CLI_CHECKSUMS: dict[str, str] = {
    "glibc-arm64": (
        "c8d5a6b174e57f06d05cb5483400b9963faf58e743b6b33706bb73ecff718047f"
        "fce6b439994255b94b5b226b90d2e91bef7418ec96c1c3f5d8bf39b6cd01321"
    ),
    "glibc-x64": (
        "3533025ba69ae112b64e3e01fbcc1ad0688136a4043f6cf6a72886967d85fdcd9"
        "ec235479c2e113171614be5225bfba93427509a05ca5aa6071d924fa7e91ca8"
    ),
    "musl-arm64": (
        "4b632e5e694f6d0825aaa57f39f99494aa6a46eac45e90d65daa10b2e46316046"
        "b627d3c1145cb9c7d58951735cc6e81c28cca44dda648b411315a28d04b2acf"
    ),
    "musl-x64": (
        "8f99c3bc478ebc2948ca69c8576a48d83b3290d807ca61fac9b0f5d0a91c0d418"
        "e595232d67e2ae0173d2188963771afd4292aefafab3f64e808a61c4399d6ed"
    ),
}
