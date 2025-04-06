import asyncio
import logging
import os
from asyncua import Client, ua
from prometheus_client import start_http_server, Gauge

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("opcua_exporter")

# Config
OPCUA_ENDPOINT = os.getenv("OPCUA_ENDPOINT", "opc.tcp://0.0.0.0:4840/freeopcua/server:4840/freeopcua/cnc/")
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "9687"))

# Métrica única y genérica para todos los nodos válidos
opcua_metric = Gauge(
    'opcua_node_value',
    'Valor de nodo OPC-UA',
    ['display_name', 'nodeid', 'namespace']
)

async def scrape_opcua():
    async with Client(url=OPCUA_ENDPOINT) as client:
        logger.info(f"Conectado a {OPCUA_ENDPOINT}")
        root = client.nodes.objects

        async def walk_and_collect(node):
            try:
                children = await node.get_children()
                for child in children:
                    await walk_and_collect(child)

                try:
                    val = await node.read_value()
                    browse_name = await node.read_browse_name()
                    nodeid = node.nodeid

                    if isinstance(val, (int, float, bool)):
                        display_name = browse_name.Name
                        opcua_metric.labels(
                            display_name=display_name,
                            nodeid=str(nodeid),
                            namespace=f"ns{nodeid.NamespaceIndex}"
                        ).set(float(val))
                except ua.UaStatusCodeError:
                    pass
                except Exception as e:
                    logger.warning(f"Error leyendo nodo: {e}")
            except Exception as e:
                logger.warning(f"Error recursivo en nodo: {e}")

        await walk_and_collect(root)


async def loop_scraper():
    while True:
        await scrape_opcua()
        await asyncio.sleep(2)

def main():
    logger.info(f"Iniciando Exporter en puerto {EXPORTER_PORT}")
    start_http_server(EXPORTER_PORT)
    asyncio.run(loop_scraper())

if __name__ == "__main__":
    main()
