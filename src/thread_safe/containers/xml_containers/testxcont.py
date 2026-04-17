import unittest

from src.thread_safe.containers.xml_containers.xcontainers import build_xml_container_tree
import xml.etree.ElementTree as ET

xml_data = """
<root xmlns:prodml="www.energistics.org">
<prodml:ProductionReport version="2.3">
  <prodml:Installation>Well_04_Production_Unit</prodml:Installation>
  <prodml:Period>2026-01-14T00:00:00Z/2026-01-15T00:00:00Z</prodml:Period>
  <prodml:ProductVolume>
    <prodml:Type>Oil</prodml:Type>
    <prodml:Volume uom="bbl">450.2</prodml:Volume>
    <prodml:Status>Measured</prodml:Status>
  </prodml:ProductVolume>
  <prodml:ProductVolume>
    <prodml:Type>Gas</prodml:Type>
    <prodml:Volume uom="mscf">1200.0</prodml:Volume>
  </prodml:ProductVolume>
</prodml:ProductionReport>
</root>"""
class Test(unittest.TestCase):

    def test_xcont(self):
        root_el = ET.fromstring(xml_data)
        tree = build_xml_container_tree(root_element=root_el)

        for n, c in tree.range_values:
            print(f"{n}:{tree.read_primitive_value(c.path)}")


