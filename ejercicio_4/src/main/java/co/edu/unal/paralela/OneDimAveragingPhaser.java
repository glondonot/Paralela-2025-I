package co.edu.unal.paralela;

import java.util.concurrent.Phaser;

/**
 * Esta clase se encarga de implementar diferentes enfoques para un algoritmo de promedio iterativo unidimensional.
 * La idea es actualizar los valores de un arreglo basándose en el promedio de sus vecinos, repitiendo este proceso
 * varias veces. Veremos una versión sencilla, una con barreras que esperan a todos, y una más avanzada
 * que usa phasers para una sincronización más flexible, buscando que el trabajo se superponga lo más posible.
 */
public final class OneDimAveragingPhaser {
    /**
     * Constructor por defecto.
     */
    private OneDimAveragingPhaser() {
    }

    /**
     * Implementación secuencial de un promedio iterativo unidimensional.
     * Calcula el promedio de cada punto basándose en sus vecinos de la iteración anterior.
     *
     * @param iterations El número de iteraciones que deben ser ejecutadas
     * @param myNew Un arreglo 'double' que inicia como el arreglo de salida
     * @param myVal Un arreglo 'double' que contiene la entrada inicial
     * del problema del promedio iterativo
     * @param n El tamaño de este problema
     */
    public static void runSequential(final int iterations, final double[] myNew,
                                     final double[] myVal, final int n) {
        double[] next = myNew;
        double[] curr = myVal;

        for (int iter = 0; iter < iterations; iter++) {
            for (int j = 1; j <= n; j++) {
                next[j] = (curr[j - 1] + curr[j + 1]) / 2.0;
            }
            // Intercambiamos los arreglos para la siguiente iteración.
            double[] tmp = curr;
            curr = next;
            next = tmp;
        }
    }

    /**
     * Un ejemplo de implementación paralela de promedio iterativo unidimensional
     * que utiliza phasers como una barrera simple (arriveAndAwaitAdvance).
     * Todos los hilos esperan en una barrera global al final de cada iteración.
     *
     * @param iterations El número de iteraciones que deben ser ejecutadas
     * @param myNew Un arreglo 'double' que inicia como el arreglo de salida
     * @param myVal Un arreglo 'double' que contiene la entrada inicial
     * del problema del promedio iterativo
     * @param n El tamaño de este problema
     * @param tasks El número de hilos/tareas para procesar
     */
    public static void runParallelBarrier(final int iterations,
                                         final double[] myNew, final double[] myVal, final int n,
                                         final int tasks) {
        // Un único Phaser centralizado para sincronizar a todos los hilos.
        Phaser ph = new Phaser(0);
        ph.bulkRegister(tasks); // Registramos a todas las tareas.

        Thread[] threads = new Thread[tasks];

        for (int ii = 0; ii < tasks; ii++) {
            final int i = ii;

            threads[ii] = new Thread(() -> {
                double[] threadPrivateMyVal = myVal;
                double[] threadPrivateMyNew = myNew;

                for (int iter = 0; iter < iterations; iter++) {
                    // Calculamos el rango de índices que este hilo va a procesar.
                    final int left = i * (n / tasks) + 1;
                    final int right = (i + 1) * (n / tasks);

                    // El hilo calcula el promedio para su porción de datos.
                    for (int j = left; j <= right; j++) {
                        threadPrivateMyNew[j] = (threadPrivateMyVal[j - 1]
                                + threadPrivateMyVal[j + 1]) / 2.0;
                    }
                    // Barrera: el hilo espera aquí hasta que todas las demás tareas
                    // hayan terminado su trabajo en esta iteración.
                    ph.arriveAndAwaitAdvance();

                    // Intercambiamos los arreglos para la siguiente iteración.
                    double[] temp = threadPrivateMyNew;
                    threadPrivateMyNew = threadPrivateMyVal;
                    threadPrivateMyVal = temp;
                }
            });
            threads[ii].start();
        }

        // El hilo principal espera a que todas las tareas se unan al final.
        for (int ii = 0; ii < tasks; ii++) {
            try {
                threads[ii].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Un ejemplo de implementación paralela de promedio iterativo unidimensional
     * que utiliza los APIs phasers.arrive y Phaser.awaitAdvance para traslapar
     * la computación con "barrier completion". Implementa una barrera difusa
     * (pipelining) donde cada hilo solo espera a sus vecinos directos.
     *
     * @param iterations El número de iteraciones que deben ser ejecutadas
     * @param myNew Un arreglo 'double' que inicia como el arreglo de salida
     * @param myVal Un arreglo 'double' que contiene la entrada inicial
     * del problema del promedio iterativo
     * @param n El tamaño de este problema
     * @param tasks El número de hilos/tareas para procesar
     */
    public static void runParallelFuzzyBarrier(final int iterations,
                                               final double[] myNew, final double[] myVal, final int n,
                                               final int tasks) {

        // Un Phaser por cada "frontera" entre hilos.
        // `phs[k]` sincroniza al hilo `k-1` (quien produce el valor `myVal[k]`)
        // con el hilo `k` (quien necesita `myVal[k]`).
        // Necesitamos `tasks + 1` phasers, desde `phs[0]` hasta `phs[tasks]`.
        // `phs[0]` es para el borde izquierdo del hilo 0.
        // `phs[tasks]` es para el borde derecho del último hilo.
        Phaser[] phs = new Phaser[tasks + 1];

        // Inicializamos todos los Phasers con 0 participantes.
        // Los hilos se irán registrando en los Phasers que les corresponden.
        for (int i = 0; i <= tasks; i++) {
            phs[i] = new Phaser(0);
        }

        Thread[] threads = new Thread[tasks];

        for (int ii = 0; ii < tasks; ii++) {
            final int i = ii;

            threads[ii] = new Thread(() -> {
                double[] threadPrivateMyVal = myVal;
                double[] threadPrivateMyNew = myNew;

                // Registramos este hilo en los phasers que le corresponden:
                // El hilo 'i' participa en 'phs[i]' (espera a su izquierda)
                // y en 'phs[i+1]' (notifica a su derecha).
                phs[i].register();
                phs[i+1].register();


                for (int iter = 0; iter < iterations; iter++) {
                    final int left = i * (n / tasks) + 1;
                    final int right = (i + 1) * (n / tasks);

                    // Si no somos el primer hilo, esperamos a que nuestro vecino izquierdo
                    // haya completado su parte del trabajo para esta fase.
                    // Esto es phs[i]. `arriveAndAwaitAdvance` mueve el phaser a la siguiente fase
                    // una vez que todos los participantes (hilo 'i-1' y hilo 'i' en este phaser) hayan llegado.
                    int currentPhase = phs[i].arriveAndAwaitAdvance();


                    // Calculamos los elementos de nuestra sección.
                    // Empezamos con el borde izquierdo, que depende de `threadPrivateMyVal[left - 1]`.
                    threadPrivateMyNew[left] = (threadPrivateMyVal[left - 1] + threadPrivateMyVal[left + 1]) / 2.0;

                    // Luego, calculamos los elementos internos de nuestra sección.
                    for (int j = left + 1; j < right; j++) {
                        threadPrivateMyNew[j] = (threadPrivateMyVal[j - 1] + threadPrivateMyVal[j + 1]) / 2.0;
                    }

                    // Después de procesar la mayor parte de nuestra sección, y específicamente
                    // después de que `threadPrivateMyNew[right]` se haya calculado (o esté a punto de calcularse),
                    // notificamos al siguiente hilo que puede avanzar.
                    // El hilo `i` notifica en `phs[i+1]`.
                    phs[i+1].arriveAndAwaitAdvance(); // Este también espera a que el hilo `i+1` lo lea

                    // Calculamos el elemento más a la derecha.
                    // Lo calculamos después de notificar, porque los vecinos ya tienen los datos previos
                    // y nosotros podemos terminar nuestra parte.
                    threadPrivateMyNew[right] = (threadPrivateMyVal[right - 1] + threadPrivateMyVal[right + 1]) / 2.0;

                    // Intercambiamos los arreglos para la siguiente iteración.
                    double[] temp = threadPrivateMyNew;
                    threadPrivateMyNew = threadPrivateMyVal;
                    threadPrivateMyVal = temp;
                }

                // Al finalizar todas las iteraciones, desregistramos este hilo de los phasers.
                // Es importante desregistrarlos para que no sigan contando en fases futuras.
                phs[i].arriveAndDeregister();
                phs[i+1].arriveAndDeregister();
            });
            threads[ii].start();
        }

        // El hilo principal espera a que todos los hilos secundarios terminen.
        for (int ii = 0; ii < tasks; ii++) {
            try {
                threads[ii].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}