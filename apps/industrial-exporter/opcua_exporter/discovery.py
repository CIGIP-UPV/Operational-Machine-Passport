import logging
from typing import List

from asyncua import Client, ua

from .models import NodeSample


LOGGER = logging.getLogger("opcua_exporter.discovery")


async def collect_numeric_samples(endpoint: str) -> List[NodeSample]:
    samples: List[NodeSample] = []
    async with Client(url=endpoint) as client:
        root = client.nodes.objects
        visited = set()

        async def walk(node, path_parts):
            try:
                nodeid = str(node.nodeid)
                if nodeid in visited:
                    return
                visited.add(nodeid)

                browse_name = await node.read_browse_name()
                current_path = [*path_parts, browse_name.Name]
                children = await node.get_children()
                for child in children:
                    await walk(child, current_path)

                value = await node.read_value()
                namespace = node.nodeid.NamespaceIndex
                if namespace == 0:
                    return
                if isinstance(value, (bool, int, float)):
                    samples.append(
                        NodeSample(
                            browse_name=browse_name.Name,
                            namespace=namespace,
                            nodeid=nodeid,
                            path="/".join(current_path),
                            value=float(value) if isinstance(value, bool) else value,
                        )
                    )
            except ua.UaStatusCodeError:
                return
            except Exception as exc:  # pragma: no cover - defensive exporter behavior
                LOGGER.warning("Error collecting node %s: %s", getattr(node, "nodeid", "unknown"), exc)

        await walk(root, [])
    return samples
