# pcloud_store.py
"""
Helper mínimo para ler/escrever um único arquivo DB JSON no pCloud,
sem criar backups.

Características:
- Usa um único arquivo remoto (por padrão /issbot/db.json) como "DB global" para todos os usuários.
- Não cria backups automáticos.
- Não usa env vars ou crypto — substitua PCLOUD_TOKEN abaixo pelo seu token pCloud.
- Expõe:
    read_db() -> Dict
    write_db(obj: Dict[str, Any]) -> Dict
    download_file_bytes(path: str) -> bytes
    upload_file_bytes(path_folder: str, filename: str, data: bytes, overwrite=True) -> dict
    get_public_url_for_db() -> str
"""

import json
import time
import requests
from typing import Optional, Dict, Any

# ---------- CONFIGURE AQUI ----------
PCLOUD_API = "https://api.pcloud.com"
# Substitua pelo seu token pCloud (coloque aqui diretamente se não quiser envs)
PCLOUD_TOKEN = "MuRLgkZbz3P7ZeaIUlazWc9F7GAuXnBCeK4WPre7y"

# Caminho remoto / pasta no pCloud onde o DB ficará (arquivo: db.json)
REMOTE_FOLDER = "/issbot"
REMOTE_DB_FILENAME = "db.json"
# ------------------------------------

if not PCLOUD_TOKEN or PCLOUD_TOKEN == "YOUR_PCLOUD_TOKEN_HERE":
    print(
        "[pcloud_store] AVISO: PCLOUD_TOKEN não configurado. "
        "Substitua PCLOUD_TOKEN em pcloud_store.py."
    )


def _uploadfile_request(
    path: str,
    filename: str,
    data: bytes,
    overwrite: bool = True,
    timeout: int = 60,
) -> dict:
    params = {"path": path, "auth": PCLOUD_TOKEN}
    if overwrite:
        params["overwrite"] = 1
    files = {"file": (filename, data, "application/json")}
    r = requests.post(
        f"{PCLOUD_API}/uploadfile", params=params, files=files, timeout=timeout
    )
    r.raise_for_status()
    return r.json()


def _getfilelink_request(
    path: str, forcedownload: int = 1, timeout: int = 30
) -> dict:
    params = {"path": path, "auth": PCLOUD_TOKEN, "forcedownload": forcedownload}
    r = requests.get(f"{PCLOUD_API}/getfilelink", params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def download_file_bytes(path: str, timeout: int = 60) -> bytes:
    j = _getfilelink_request(path, forcedownload=1, timeout=timeout)
    if not isinstance(j, dict) or j.get("result", 0) != 0:
        raise RuntimeError(f"pCloud getfilelink error: {j}")
    hosts = j.get("hosts")
    link_path = j.get("path")
    if not hosts or not link_path:
        raise RuntimeError(f"pCloud getfilelink inválido: {j}")
    host = hosts[0].rstrip("/")
    url = f"https://{host}{link_path}"
    r = requests.get(url, stream=True, timeout=timeout)
    r.raise_for_status()
    return r.content


def upload_file_bytes(
    path_folder: str,
    filename: str,
    data: bytes,
    overwrite: bool = True,
    timeout: int = 90,
) -> dict:
    j = _uploadfile_request(path_folder, filename, data, overwrite=overwrite, timeout=timeout)
    if not isinstance(j, dict) or j.get("result", 0) != 0:
        raise RuntimeError(f"pCloud upload error: {j}")
    return j


def _remote_db_path() -> str:
    return REMOTE_FOLDER + "/" + REMOTE_DB_FILENAME


def read_db() -> Dict[str, Any]:
    """
    Lê o JSON remoto e retorna um dict.
    Se não existir ou der erro, retorna {'version': 0, 'data': {}}.
    """
    path = _remote_db_path()
    try:
        raw = download_file_bytes(path)
        obj = json.loads(raw.decode("utf-8"))
        if not isinstance(obj, dict):
            return {"version": 0, "data": {}}
        obj.setdefault("version", 0)
        obj.setdefault("data", {})
        return obj
    except Exception as e:
        print(f"[pcloud_store] read_db: não foi possível baixar '{path}': {e}")
        return {"version": 0, "data": {}}


def write_db(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Grava o dict como JSON em /issbot/db.json, atualizando a versão.
    Não cria backups.
    Retorna metadados: {'upload': ..., 'version': new_version}.
    """
    if not isinstance(obj, dict):
        raise ValueError("obj deve ser um dict JSON-serializável")

    # carrega versão atual (se houver)
    remote_path = _remote_db_path()
    try:
        raw = download_file_bytes(remote_path)
        existing = json.loads(raw.decode("utf-8"))
        current_version = int(existing.get("version", 0)) if isinstance(existing, dict) else 0
    except Exception:
        current_version = 0

    new_version = current_version + 1
    obj_to_save = dict(obj)
    obj_to_save["version"] = new_version
    obj_to_save["last_saved_ts"] = int(time.time())

    payload_bytes = json.dumps(obj_to_save, ensure_ascii=False).encode("utf-8")

    upload_meta = upload_file_bytes(REMOTE_FOLDER, REMOTE_DB_FILENAME, payload_bytes, overwrite=True)
    print(f"[pcloud_store] DB salvo: {REMOTE_FOLDER}/{REMOTE_DB_FILENAME} (version={new_version})")

    return {"upload": upload_meta, "version": new_version}


def get_public_url_for_db() -> str:
    """
    Retorna URL pública para download direto de /issbot/db.json.
    """
    j = _getfilelink_request(_remote_db_path(), forcedownload=1)
    if j.get("result", 0) != 0:
        raise RuntimeError(f"pCloud getfilelink error: {j}")
    hosts = j.get("hosts")
    link_path = j.get("path")
    if not hosts or not link_path:
        raise RuntimeError(f"pCloud getfilelink inválido: {j}")
    host = hosts[0].rstrip("/")
    return f"https://{host}{link_path}"


if __name__ == "__main__":
    print("pcloud_store — utilitários para DB único no pCloud")
    print("REMOTE DB:", REMOTE_FOLDER + "/" + REMOTE_DB_FILENAME)
    try:
        db = read_db()
        v = db.get("version", 0)
        print("DB lido com sucesso, version=", v)
        print("Para gravar um teste, chame write_db({…}).")
    except Exception as e:
        print("Erro ao ler DB:", e)
