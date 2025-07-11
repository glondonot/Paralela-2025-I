package co.edu.unal.paralela;

import static edu.rice.pcdp.PCDP.forseq2d;
import static edu.rice.pcdp.PCDP.forall2dChunked; // Necesitamos importar forallChunked

/**
 * Clase envolvente pata implementar de forma eficiente la multiplicación dde matrices en paralelo.
 */
public final class MatrixMultiply {
    /**
     * Constructor por omisión.
     */
    private MatrixMultiply() {
    }

    /**
     * Realiza una multiplicación de matrices bidimensionales (A x B = C) de forma secuencial.
     *
     * @param A Una matriz de entrada con dimensiones NxN
     * @param B Una matriz de entrada con dimensiones NxN
     * @param C Matriz de salida
     * @param N Tamaño de las matrices de entrada
     */
    public static void seqMatrixMultiply(final double[][] A, final double[][] B,
            final double[][] C, final int N) {
        forseq2d(0, N - 1, 0, N - 1, (i, j) -> {
            C[i][j] = 0.0;
            for (int k = 0; k < N; k++) {
                C[i][j] += A[i][k] * B[k][j];
            }
        });
    }

    /**
     * Realiza una multiplicación de matrices bidimensionales (A x B = C) de forma paralela
     * utilizando el método 'forall' de PCDP.
     *
     * @param A Una matriz de entrada con dimensiones NxN
     * @param B Una matriz de entrada con dimensiones NxN
     * @param C Matriz de salida
     * @param N Tamaño de las matrices de entrada
     */
    public static void parMatrixMultiply(final double[][] A, final double[][] B,
                                         final double[][] C, final int N) {
        // Aqui es donde hacemos el cambio clave. En lugar de usar 'forseq2d' que ejecuta
        // el bucle de forma secuencial, usamos 'forall2dChunked'. Este se encarga
        // de dividir las iteraciones (i y j en este caso) y asignarlas a diferentes
        // hilos para que se ejecuten en paralelo  (el metododo forall2dChunked divide
        // las iteraciones en "pedazos" (chunks) más grandes que el metodo forall2d).
        // Como la computación de cada elemento C[i][j] es independiente de los demás,
        // esto es perfecto para el paralelismo en ciclos.
        forall2dChunked(0, N - 1, 0, N - 1, (i, j) -> {
            C[i][j] = 0.0;
            for (int k = 0; k < N; k++) {
                C[i][j] += A[i][k] * B[k][j];
            }
        });
    }
}
