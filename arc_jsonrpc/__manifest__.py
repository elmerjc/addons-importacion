# -*- coding: utf-8 -*-
{
    'name' : 'Importación de datos con JSON RPC',
    'version' : '1.0',
    'category': 'Server',
    'author' : 'takana.cloud',
    'website': "https://takana.cloud",
    'summary' : 'Métodos para sincronizar datos desde otro servidor',
    'description' : """
    - Registros de comprobantes (account.move)
    """,
    'depends' : [
        'base',
        'account'
    ],
    'data' : [
        'security/ir.model.access.csv',
        'wizard/sync_data_view.xml',
        'views/json_rpc_view.xml',
    ],
    'license': 'LGPL-3',
    'sequence': 1,
    'installable' : True,
    'auto_install': False,
    'aplication' : True
}
