/*
import scala.collection.mutable.ArrayBuffer

class LayerNW(perceptrons: Int) {
  var weights = ArrayBuffer[Double]()

  // Заполняем веса случайными числами
  def generateWeights() {
    val r = scala.util.Random
    for (i <- 0 to perceptrons) {
      weights(i) += r.nextDouble()
    }
  }
}
class NeuralNW(input: Int, perseptrons: Int, output: Int)
{
  val layers = ArrayBuffer[LayerNW]()

  layers += new LayerNW(input)
  layers += new LayerNW(perseptrons)
  layers += new LayerNW(output)

  layers.foreach(layer => layer.generateWeights())

  var netout = ArrayBuffer[Double]()

  def openNW(fileName: String)
  {

  }

  def saveNW(fileName: String)
  {

  }


  def netOUT(jLayer: Int): Double =
  {
    var output = 0.0

    for (weight <- layers(jLayer).weights)
    {
      output = netout(jLayer)
    }

    return output
  }


    def calcError(Y: ArrayBuffer[Double]): Double =
    {
      var kErr = 0.0
      for (i <- output)
      {
        kErr += math.pow(Y(i) - netOUT(layers.length), 2)
      }

      return 0.5 * kErr;
    }

    /* Обучает сеть, изменяя ее весовые коэффициэнты.
       X, Y - обучающая пара. kLern - скорость обучаемости
       В качестве результата метод возвращает ошибку 0.5(Y-outY)^2 */
    def lernNW(X: ArrayBuffer[Double], Y: ArrayBuffer[Double], kLern: Double)
    {
      // Вычисляем выход сети
      getOUT(X, layers.length)

      // Заполняем дельта последнего слоя
      for (int j = 0; j < Layers[countLayers - 1].countY; j++)
      {
        O = NETOUT[countLayers][j];
        DELTA[countLayers - 1][j] = (Y[j] - O) * O * (1 - O);
      }

      // Перебираем все слои начиная споследнего
      // изменяя веса и вычисляя дельта для скрытого слоя
      for (int k = countLayers - 1; k >= 0; k--)
      {
        // Изменяем веса выходного слоя
        for (int j = 0; j < Layers[k].countY; j++)
        {
          for (int i = 0; i < Layers[k].countX; i++)
          {
            Layers[k][i, j] += kLern * DELTA[k][j] * NETOUT[k][i];
          }
        }
        if (k > 0)
        {

          // Вычисляем дельта слоя к-1
          for (int j = 0; j < Layers[k - 1].countY; j++)
          {

            s = 0;
            for (int i = 0; i < Layers[k].countY; i++)
            {
              s += Layers[k][j, i] * DELTA[k][i];
            }

            DELTA[k - 1][j] = NETOUT[k][j] * (1 - NETOUT[k][j]) * s;
          }
        }
      }

      return calcError(X, Y);
    }


    // Возвращает все значения нейронов до lastLayer слоя
    def getOUT(inX: Array[Double], lastLayer: Int)
    {

      for (int j = 0; j < Layers[0].countX; j++)
      NETOUT[0][j] = inX[j];

      for (int i = 0; i < lastLayer; i++)
      {
        // размерность столбца проходящего через i-й слой
        for (int j = 0; j < Layers[i].countY; j++)
        {
          s = 0;
          for (int k = 0; k < Layers[i].countX; k++)
          {
            s += Layers[i][k, j] * NETOUT[i][k];
          }

          // Вычисляем значение активационной функции
          s = 1.0 / (1 + Math.Exp(-s));
          NETOUT[i + 1][j] = 0.998 * s + 0.001;

        }
      }

    }





  }
  }
*/