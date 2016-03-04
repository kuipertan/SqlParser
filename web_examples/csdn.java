public String removeOrderBy(String sql) throws JSQLParserException {  
    Statement stmt = CCJSqlParserUtil.parse(sql);  
    Select select = (Select) stmt;  
    SelectBody selectBody = select.getSelectBody();  
    processSelectBody(selectBody);  
    return select.toString();  
}  
  
public void processSelectBody(SelectBody selectBody) {  
    if (selectBody instanceof PlainSelect) {  
        processPlainSelect((PlainSelect) selectBody);  
    } else if (selectBody instanceof WithItem) {  
        WithItem withItem = (WithItem) selectBody;  
        if (withItem.getSelectBody() != null) {  
            processSelectBody(withItem.getSelectBody());  
        }  
    } else {  
        SetOperationList operationList = (SetOperationList) selectBody;  
        if (operationList.getPlainSelects() != null && operationList.getPlainSelects().size() > 0) {  
            List<PlainSelect> plainSelects = operationList.getPlainSelects();  
            for (PlainSelect plainSelect : plainSelects) {  
                processPlainSelect(plainSelect);  
            }  
        }  
        if (!orderByHashParameters(operationList.getOrderByElements())) {  
            operationList.setOrderByElements(null);  
        }  
    }  
}  
  
public void processPlainSelect(PlainSelect plainSelect) {  
    if (!orderByHashParameters(plainSelect.getOrderByElements())) {  
        plainSelect.setOrderByElements(null);  
    }  
    if (plainSelect.getFromItem() != null) {  
        processFromItem(plainSelect.getFromItem());  
    }  
    if (plainSelect.getJoins() != null && plainSelect.getJoins().size() > 0) {  
        List<Join> joins = plainSelect.getJoins();  
        for (Join join : joins) {  
            if (join.getRightItem() != null) {  
                processFromItem(join.getRightItem());  
            }  
        }  
    }  
}  
  
public void processFromItem(FromItem fromItem) {  
    if (fromItem instanceof SubJoin) {  
        SubJoin subJoin = (SubJoin) fromItem;  
        if (subJoin.getJoin() != null) {  
            if (subJoin.getJoin().getRightItem() != null) {  
                processFromItem(subJoin.getJoin().getRightItem());  
            }  
        }  
        if (subJoin.getLeft() != null) {  
            processFromItem(subJoin.getLeft());  
        }  
    } else if (fromItem instanceof SubSelect) {  
        SubSelect subSelect = (SubSelect) fromItem;  
        if (subSelect.getSelectBody() != null) {  
            processSelectBody(subSelect.getSelectBody());  
        }  
    } else if (fromItem instanceof ValuesList) {  
  
    } else if (fromItem instanceof LateralSubSelect) {  
        LateralSubSelect lateralSubSelect = (LateralSubSelect) fromItem;  
        if (lateralSubSelect.getSubSelect() != null) {  
            SubSelect subSelect = (SubSelect) (lateralSubSelect.getSubSelect());  
            if (subSelect.getSelectBody() != null) {  
                processSelectBody(subSelect.getSelectBody());  
            }  
        }  
    }  
    //Table时不用处理  
}  
  
public boolean orderByHashParameters(List<OrderByElement> orderByElements) {  
    if (orderByElements == null) {  
        return false;  
    }  
    for (OrderByElement orderByElement : orderByElements) {  
        if (orderByElement.toString().toUpperCase().contains("?")) {  
            return true;  
        }  
    }  
    return false;  
}  
