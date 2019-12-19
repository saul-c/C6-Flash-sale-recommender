package stormapplied.flashsale.topology;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import stormapplied.flashsale.metrics.MultiSuccessRateMetric;

import stormapplied.flashsale.services.FlashSaleClient;
import stormapplied.flashsale.domain.Sale;
import stormapplied.flashsale.services.Timeout;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/**
 * 查询商品的详情信息
 */
public class LookupSalesDetails extends BaseRichBolt {
  private final static int TIMEOUT = 100;
  private FlashSaleClient client;
  private OutputCollector outputCollector;

  private final int METRICS_WINDOW = 15;
  private transient MultiSuccessRateMetric sucessRates;

  @Override
  public void prepare(Map config,
                      TopologyContext context,
                      OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
    client = new FlashSaleClient(TIMEOUT);
    //监控成功指标
    sucessRates = new MultiSuccessRateMetric();
    context.registerMetric("sales-lookup-success-rate", sucessRates, METRICS_WINDOW);    
  }

  @Override
  public void execute(Tuple tuple) {
    String customerId = tuple.getStringByField("customer");
    List<String> saleIds = (List<String>) tuple.getValueByField("sales");

    List<Sale> sales = new ArrayList<Sale>();
    for (String saleId: saleIds) {
      try {
        //为客户迭代查找商品详情
        Sale sale = client.lookupSale(saleId);
        sales.add(sale);
      } catch (Timeout e) {
        sucessRates.scope(customerId).incrFail(1);
        //查询某个客户异常，但其他客户继续执行
        outputCollector.reportError(e);
      }
    }

    if (sales.isEmpty()) {
      outputCollector.fail(tuple);
    } else {
      //记录成功指标率
      sucessRates.scope(customerId).incrSuccess(sales.size());
      //将成功查询到商品详情的客户组成新的元祖发送到一下处理
      outputCollector.emit(new Values(customerId, sales));
      //有商品详情应答输入元祖
      outputCollector.ack(tuple);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("customer", "sales"));
  }
}
