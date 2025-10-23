# -*- coding: utf-8 -*-

import logging
import xlsxwriter

from io import BytesIO

from odoo import http
from odoo.http import content_disposition, request

_logger = logging.getLogger(__name__)


class ReportController(http.Controller):
     
    @http.route('/report/products', type='http')
    def reporte_productos(self, company_id, **kw):
        product_ids = request.env['product.product'].sudo().search([
            ('sale_ok', '=', True),
            ('company_id', '=', int(company_id)),
            ('minicode', '!=', False),
        ], order='minicode')

        company = request.env['res.company'].search([
            ('id', '=', company_id)
        ], limit=1)
        
        if not company:
            return "<h3>No existe datos de la empresa</h3>"

        if len(product_ids) == 0:
            return "<h3>No existen datos para el reporte</h3>"
        else:
            return self.render_excel(product_ids, company)

    def render_excel(self, datos, company):
        if len(datos):
            excel = BytesIO()
            workbook = xlsxwriter.Workbook(excel)

            format_header = workbook.add_format({
                'bold': True,
                'border': True,
                'font_name': 'Calibri',
                'font_size': 11,
                'align': 'center',
                'valign': 'vcenter',
                #'bg_color': '#efa9db'
            })
            format_body = workbook.add_format({
                'font_size': 11
            })
            format_number = workbook.add_format({
                'num_format': '#,##0.00'
            })

            sheet = workbook.add_worksheet(u'Productos')

            sheet.set_column('A:A', 75)
            sheet.set_column('B:B', 20)
            sheet.set_column('C:C', 30)
            sheet.set_column('D:D', 30)
            sheet.set_column('E:E', 12)
            sheet.set_column('F:F', 15)
            sheet.set_column('G:G', 15)
            sheet.set_column('H:H', 15)
            sheet.set_column('I:I', 70)
            sheet.set_column('J:J', 20)
            sheet.set_column('K:K', 20)
            sheet.set_column('L:L', 15)
            sheet.set_column('M:M', 20)
            sheet.set_column('N:N', 12)
            sheet.set_column('O:O', 15)
            sheet.set_column('P:P', 15)
            sheet.set_column('Q:Q', 15)
            sheet.set_column('R:R', 15)

            sheet.write_row(0, 0,
                           (u'0 - Producto',
                            u'1 - Cantidad',
                            u'2 - Seguimiento con Serie',
                            u'3 - Referencia Interna',
                            u'4 - Costo',
                            u'5 - Precio base',
                            u'6 - Precio oferta',
                            u'7 - Precio venta',
                            u'8 - Descripción',
                            u'9 - Categoria',
                            u'10 - Subcategoria',
                            u'11 - Tecnologia',
                            u'12 - Marca',
                            u'13 - Publico',
                            u'14 - Modelo',
                            u'15 - Minicodigo',
                            u'16 - Garantia',
                            u'17 - Disponibilidad',
            ), format_header)

            row = 0
            
            for rec in datos:
                column_number = 0
                row += 1
                sheet.write(row, column_number, rec.name, format_body)
                column_number += 1
                sheet.write(row, column_number, "", format_body)
                column_number += 1
                sheet.write(row, column_number, "1" if rec.tracking == "serial" else "0", format_body)
                column_number += 1
                sheet.write(row, column_number, rec.default_code, format_number)
                column_number += 1
                sheet.write(row, column_number, rec.standard_price, format_number)
                column_number += 1
                sheet.write(row, column_number, rec.list_price, format_number)
                column_number += 1
                sheet.write(row, column_number, 0.00, format_number)
                column_number += 1
                sheet.write(row, column_number, 0.00, format_number)
                column_number += 1
                sheet.write(row, column_number, rec.description_sale or "", format_body)
                column_number += 1
                sheet.write(row, column_number, rec.product_tmpl_id.categ_id.parent_id.name if rec.product_tmpl_id.categ_id.parent_id else "", format_body)
                column_number += 1
                sheet.write(row, column_number, rec.product_tmpl_id.categ_id.name if rec.product_tmpl_id.categ_id else "", format_body)
                column_number += 1
                sheet.write(row, column_number, rec.tecnology or "", format_body)
                column_number += 1
                sheet.write(row, column_number, "", format_body)
                column_number += 1
                sheet.write(row, column_number, "", format_body)
                column_number += 1
                sheet.write(row, column_number, rec.model or "", format_body)
                column_number += 1
                sheet.write(row, column_number, rec.minicode, format_body)

            workbook.close()

            name_file = '%s Lista de productos' % (company.name)
            response = request.make_response(
                    excel.getvalue(),
                    headers=[
                        ('Content-Type', 'application/vnd.ms-excel'),
                        ('Content-Disposition', content_disposition(name_file + '.xlsx'))
                    ]
                )
            excel.close()

            return response
