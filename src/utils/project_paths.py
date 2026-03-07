#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 13 10:55:14 2025

@author: mlefevre
"""

from pathlib import Path
import platform
import socket
import logging

log = logging.getLogger(__name__)

def get_project_root(prefer_nas: bool = True) -> Path:
    """
    Return the root directory for the research project.

    Parameters
    ----------
    prefer_nas : bool, optional
        If True (default), try NAS first and fall back to local.
        If False, use local paths even if NAS is mounted.

    Works across macOS and Windows.
    """
    hostname = socket.gethostname()
    system = platform.system()
    log.debug("Hostname: %s", hostname)
    log.debug("System: %s", system)

    # --- 1. NAS paths ---
    nas_candidates = [
        Path(r"\\Nas\nas1\Documents\Education\2021 EDHEC Exec PhD\4 Research"),   # Windows UNC
        Path("/volumes/NAS1/Documents/Education/2021 EDHEC Exec PhD/4 Research"), # macOS mount
    ]

    if prefer_nas:
        for nas_path in nas_candidates:
            if nas_path.exists():
                log.info("Using NAS path: %s", nas_path)
                return nas_path

    # --- 2. Local user Documents folder ---
    home = Path.home()
    relative_to_user_home = Path("Documents/Education/2021 EDHEC Exec PhD/4 Research")

    if system == "Windows":
        project_root = home / "OneDrive" / relative_to_user_home
    elif system == "Darwin":  # macOS
        project_root = home / relative_to_user_home
    else:
        raise OSError(f"Unsupported OS: {system}")

    project_root.mkdir(parents=True, exist_ok=True)
    log.info("Using local path: %s", project_root)
    return project_root


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    log.info("=== Default (prefer NAS) ===")
    root = get_project_root()
    log.info("Project root: %s", root)
    log.info("Directory exists: %s", root.exists())

    log.info("=== Force local path ===")
    root_local = get_project_root(prefer_nas=False)
    log.info("Project root: %s", root_local)
    log.info("Directory exists: %s", root_local.exists())