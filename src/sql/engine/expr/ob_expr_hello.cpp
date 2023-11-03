/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "sql/engine/expr/ob_expr_hello.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprHello::ObExprHello(ObIAllocator &alloc)
  : ObStringExprOperator(alloc, T_HELLO, "hello", 0, NOT_VALID_FOR_GENERATED_COL)
{
}

ObExprHello::~ObExprHello()
{
}

int ObExprHello::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_varchar();
  type.set_length(static_cast<common::ObLength>(strlen("Hello, OceanBase Competition!")));
  type.set_default_collation_type();
  type.set_collation_level(CS_LEVEL_SYSCONST);
  return OB_SUCCESS;
}

int ObExprHello::eval_hello(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         ObDatum &expr_datum)
{
  UNUSED(expr);
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    expr_datum.set_string(common::ObString("Hello, OceanBase Competition!"));
  }
  return ret;
}

int  ObExprHello::cg_expr(ObExprCGCtx &op_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = ObExprHello::eval_hello;
  return ret;
}

} // namespace sql
} // namespace oceanbase
