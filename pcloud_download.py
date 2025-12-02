import os
import logging
from typing import Callable, Any, Dict

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse
import requests

logger = logging.getLogger("pcloud_download")
logger.setLevel(logging.INFO)

# pCloud API base URL and token configuration
API_BASE = "https://api.pcloud.com"
TOKEN = os.getenv("PCLOUD_TOKEN") or "MuRLgkZbz3P7ZeaIUlazWc9F7GAuXnBCeK4WPre7y"


def pcloud_api_json(endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Faz chamada GET ao endpoint pCloud retornando JSON decodificado.
    """
    params = params.copy()
    params['auth'] = TOKEN
    url = f"{API_BASE}/{endpoint}"
    logger.debug(f"pCloud GET JSON {url} params={params}")
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data.get('result') != 0:
        raise HTTPException(500, f"pCloud API error: {data.get('error')}")
    return data


def pcloud_api_stream(endpoint: str, params: Dict[str, Any]) -> requests.Response:
    """
    Faz chamada GET ao endpoint pCloud retornando a Response em streaming.
    """
    params = params.copy()
    params['auth'] = TOKEN
    url = f"{API_BASE}/{endpoint}"
    logger.debug(f"pCloud GET stream {url} params={params}")
    resp = requests.get(url, params=params, stream=True, timeout=60)
    content_type = resp.headers.get('Content-Type', '')
    if 'application/json' in content_type:
        data = resp.json()
        if data.get('result') != 0:
            raise HTTPException(500, f"pCloud API error: {data.get('error')}")
    if resp.status_code != 200:
        raise HTTPException(resp.status_code, f"pCloud API HTTP {resp.status_code}")
    return resp


def make_pcloud_router(
    get_session_func: Callable[[Request], Any],
    route_path: str = "/api/download_zip",
) -> APIRouter:
    """
    Cria APIRouter com endpoint para gerar ZIP pelos serviços pCloud.
    - get_session_func(request) -> dict com 'colaborador_norm'
    """
    router = APIRouter()

    @router.get(route_path)
    async def api_download_zip(request: Request):
        # Autenticação da sessão
        try:
            maybe = get_session_func(request)
            sess = await maybe if hasattr(maybe, '__await__') else maybe
        except Exception as e:
            logger.exception("Erro em get_session_func: %s", e)
            raise HTTPException(401, "Não autenticado")
        if not sess or not sess.get('colaborador_norm'):
            raise HTTPException(401, "Sessão inválida ou colaborador não definido")

        colaborator = sess['colaborador_norm'].strip()
        if not colaborator:
            raise HTTPException(400, "Nome de colaborador inválido")

        # 1) Descobre folderid via listfolder
        folder_path = f"/issbot/{colaborator}"
        try:
            data = pcloud_api_json('listfolder', {'path': folder_path})
            folderid = data['metadata'].get('folderid')
            if not folderid:
                raise KeyError("folderid não encontrado")
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Erro ao obter folderid pCloud para %s: %s", folder_path, e)
            raise HTTPException(404, f"Pasta pCloud não encontrada: {colaborator}")

        logger.info("Gerando ZIP pCloud para folderid=%s", folderid)

        # 2) Chama getzip com folderids
        params = {
            'folderids': folderid,
            'forcedownload': 1,
            'filename': f'{colaborator}.zip'
        }
        try:
            resp = pcloud_api_stream('getzip', params)
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Erro ao solicitar ZIP: %s", e)
            raise HTTPException(500, f"Erro ao solicitar ZIP pCloud: {e}")

        # Stream direto o ZIP
        headers = {'Content-Disposition': f'attachment; filename="{colaborator}.zip"'}
        return StreamingResponse(resp.iter_content(chunk_size=64*1024), media_type='application/zip', headers=headers)

    return router


__all__ = ["make_pcloud_router"]
