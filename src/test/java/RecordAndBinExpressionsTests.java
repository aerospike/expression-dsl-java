import com.aerospike.ConditionTranslator;
import com.aerospike.client.exp.Expression;
import org.junit.jupiter.api.Test;

public class RecordAndBinExpressionsTests {

    @Test
    void intBinGT() {
        translateAndPrint("$.intBin1 > 10");
    }

    @Test
    void stringBinEquals() {
        translateAndPrint("$.strBin == \"yes\"");
    }

    @Test
    void testAnd() {
        translateAndPrint("$.a.exists() and $.b.exists()");
    }

    private void translateAndPrint(String input) {
        Expression expression = ConditionTranslator.translate(input);
        System.out.println(expression);
    }
}
