# -*- coding: utf-8 -*-
{
    'name': 'Importación de Inventarios',
    'version': '1.0.0',
    'summary': """ Importación de inventarios desde un archivo excel """,
    'author': 'takana.cloud',
    'website': 'https://takana.cloud',
    'category': 'Stock',
    'depends': [
        'base',
        'sh_message',
        'stock',
        'sale_stock',
        'arc_product_import',
        'stock_inventory'
    ],
    "data": [
        "security/ir.model.access.csv",
        "wizards/wizard_inventory_variants_import.xml",
        "wizards/wizard_inventory_import.xml"
    ],
    'sequence': 1,
    'application': True,
    'installable': True,
    'auto_install': False,
    'license': 'LGPL-3',
}
