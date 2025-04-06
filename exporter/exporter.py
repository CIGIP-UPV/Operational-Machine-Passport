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

# Almacén de métricas
metrics = {}


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
                    nodeid = node.nodeid.to_string()

                    label = f"{browse_name.Name}_{nodeid}".replace(" ", "_") \
                                                          .replace(";", "_") \
                                                          .replace("=", "_") \
                                                          .replace(".", "_") \
                                                          .replace(":", "_") \
                                                          .replace("-", "_")

                    if isinstance(val, (int, float, bool)):
                        if label not in metrics:
                            metrics[label] = Gauge(label, f"OPC-UA variable from {nodeid}")
                        metrics[label].set(float(val))  # bools también se pueden castear a float (0/1)
                except ua.UaStatusCodeError:
                    pass  # Sin valor
                except Exception as e:
                    logger.warning(f"Error leyendo valor del nodo: {e}")
            except Exception as e:
                logger.warning(f"Error procesando nodo: {e}")

        await walk_and_collect(root)


async def loop_scraper():
    while True:
        await scrape_opcua()
        await asyncio.sleep(2)  # Intervalo de scraping


def main():
    logger.info(f"Iniciando Prometheus Exporter en puerto {EXPORTER_PORT}")
    start_http_server(EXPORTER_PORT)
    asyncio.run(loop_scraper())


if __name__ == "__main__":
    main()
