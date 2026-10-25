import unittest

from thread_safe.containers.xml_containers.xcontainers import build_xml_container_tree
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

        paths = {n: tree.read_primitive_value(c.path) for n, c in tree.range_containers}
        self.assertEqual(paths["root.ProductionReport_1.Installation_1"], "Well_04_Production_Unit")
        self.assertEqual(paths["root.ProductionReport_1.ProductVolume_1.Type_1"], "Oil")
        self.assertEqual(paths["root.ProductionReport_1.ProductVolume_2.Type_1"], "Gas")
        self.assertEqual(tree.read_primitive_value("root.ProductionReport_1@version"), "2.3")
        self.assertEqual(tree.read_primitive_value("root.ProductionReport_1.ProductVolume_1.Volume_1@uom"), "bbl")
        self.assertEqual(tree.read_primitive_value("root.ProductionReport_1.ProductVolume_2.Volume_1@uom"), "mscf")
        self.assertIsNotNone(tree.read_from_value("root.ProductionReport_1.Installation_1"))


