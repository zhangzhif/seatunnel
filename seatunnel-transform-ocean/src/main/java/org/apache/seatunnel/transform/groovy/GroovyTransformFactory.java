// package org.apache.seatunnel.transform.groovy;
//
// import org.apache.seatunnel.api.configuration.ReadonlyConfig;
// import org.apache.seatunnel.api.configuration.util.OptionRule;
// import org.apache.seatunnel.api.table.catalog.CatalogTable;
// import org.apache.seatunnel.api.table.connector.TableTransform;
// import org.apache.seatunnel.api.table.factory.Factory;
// import org.apache.seatunnel.api.table.factory.TableFactoryContext;
// import org.apache.seatunnel.api.table.factory.TableTransformFactory;
//
// import com.google.auto.service.AutoService;
//
// @AutoService(Factory.class)
// public class GroovyTransformFactory implements TableTransformFactory {
//    @Override
//    public String factoryIdentifier() {
//        return GroovyTransform.PLUGIN_NAME;
//    }
//
//    @Override
//    public OptionRule optionRule() {
//        return OptionRule.builder().required(GroovyTransformConfig.CODE).build();
//    }
//
//    @Override
//    public TableTransform createTransform(TableFactoryContext context) {
//        CatalogTable catalogTable = context.getCatalogTable();
//        ReadonlyConfig config = context.getOptions();
//        return () -> new GroovyTransform(config, catalogTable);
//    }
// }
