"""
Performance Metrics Module

This module contains utilities for monitoring and displaying
performance metrics throughout the pipeline execution.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

import time
import psutil
import os
from pathlib import Path
from typing import Dict, Any


class PerformanceMetrics:
    """
    Class responsible for collecting and displaying performance metrics
    """
    
    def __init__(self):
        """Initialize performance metrics collector"""
        self.metrics = {}
        self.start_times = {}
        self.process = psutil.Process()
    
    def start_timer(self, operation_name: str):
        """Start timing an operation"""
        self.start_times[operation_name] = time.time()
    
    def end_timer(self, operation_name: str):
        """End timing an operation and store the result"""
        if operation_name in self.start_times:
            duration = time.time() - self.start_times[operation_name]
            self.metrics[operation_name] = duration
            del self.start_times[operation_name]
            return duration
        return 0
    
    def get_system_metrics(self):
        """Get current system metrics"""
        return {
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory_percent": psutil.virtual_memory().percent,
            "memory_available_gb": psutil.virtual_memory().available / (1024**3),
            "disk_usage_percent": psutil.disk_usage('/').percent if os.name != 'nt' else psutil.disk_usage('C:').percent
        }
    
    def get_process_metrics(self):
        """Get current process metrics"""
        try:
            memory_info = self.process.memory_info()
            return {
                "process_memory_mb": memory_info.rss / (1024**2),
                "process_cpu_percent": self.process.cpu_percent(),
                "num_threads": self.process.num_threads()
            }
        except Exception:
            return {
                "process_memory_mb": 0,
                "process_cpu_percent": 0,
                "num_threads": 0
            }
    
    def calculate_performance_score(self):
        """Calculate overall performance score"""
        system_metrics = self.get_system_metrics()
        
        # Base score
        score = 100
        
        # Penalize high CPU usage
        if system_metrics["cpu_percent"] > 80:
            score -= 20
        elif system_metrics["cpu_percent"] > 60:
            score -= 10
        
        # Penalize high memory usage
        if system_metrics["memory_percent"] > 90:
            score -= 30
        elif system_metrics["memory_percent"] > 75:
            score -= 15
        
        # Penalize high disk usage
        if system_metrics["disk_usage_percent"] > 95:
            score -= 20
        elif system_metrics["disk_usage_percent"] > 85:
            score -= 10
        
        return max(0, score)
    
    def show_current_metrics(self):
        """Show current performance metrics"""
        print("\n" + "=" * 60)
        print("📈 MÉTRICAS DE RENDIMIENTO ACTUALES")
        print("=" * 60)
        
        # System metrics
        system_metrics = self.get_system_metrics()
        print(f"🖥️ CPU: {system_metrics['cpu_percent']:.1f}%")
        print(f"🧠 Memoria: {system_metrics['memory_percent']:.1f}% (Disponible: {system_metrics['memory_available_gb']:.1f}GB)")
        print(f"💾 Disco: {system_metrics['disk_usage_percent']:.1f}%")
        
        # Process metrics
        process_metrics = self.get_process_metrics()
        print(f"⚡ Proceso - Memoria: {process_metrics['process_memory_mb']:.1f}MB")
        print(f"⚡ Proceso - CPU: {process_metrics['process_cpu_percent']:.1f}%")
        print(f"⚡ Proceso - Hilos: {process_metrics['num_threads']}")
        
        # Performance score
        score = self.calculate_performance_score()
        print(f"🎯 Score de rendimiento: {score:.1f}/100")
        
        # Execution times if available
        if self.metrics:
            print("\n⏱️ TIEMPOS DE EJECUCIÓN:")
            for operation, duration in self.metrics.items():
                print(f"   • {operation}: {duration:.2f}s")
    
    def show_metrics_comparison(self, other_metrics: Dict[str, Any]):
        """Show comparison between current and other metrics"""
        print("\n" + "=" * 60)
        print("🔄 COMPARACIÓN DE MÉTRICAS")
        print("=" * 60)
        
        current_total = sum(self.metrics.values()) if self.metrics else 0
        other_total = sum(other_metrics.values()) if other_metrics else 0
        
        if current_total > 0 and other_total > 0:
            improvement = ((other_total - current_total) / other_total) * 100
            print(f"📊 Tiempo total actual: {current_total:.2f}s")
            print(f"📊 Tiempo total anterior: {other_total:.2f}s")
            
            if improvement > 0:
                print(f"✅ Mejora: {improvement:.1f}% más rápido")
            else:
                print(f"⚠️ Degradación: {abs(improvement):.1f}% más lento")
        
        # Show individual operation comparisons
        for operation in set(self.metrics.keys()) | set(other_metrics.keys()):
            current_time = self.metrics.get(operation, 0)
            other_time = other_metrics.get(operation, 0)
            
            if current_time > 0 and other_time > 0:
                op_improvement = ((other_time - current_time) / other_time) * 100
                status = "✅" if op_improvement > 0 else "⚠️"
                print(f"   {status} {operation}: {current_time:.2f}s vs {other_time:.2f}s ({op_improvement:+.1f}%)")
    
    def export_metrics_report(self, filepath: str = "performance_report.txt"):
        """Export metrics to a text report"""
        report_path = Path(filepath)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("REPORTE DE MÉTRICAS DE RENDIMIENTO\n")
            f.write("=" * 50 + "\n")
            f.write(f"Fecha: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # System metrics
            system_metrics = self.get_system_metrics()
            f.write("MÉTRICAS DEL SISTEMA:\n")
            f.write(f"CPU: {system_metrics['cpu_percent']:.1f}%\n")
            f.write(f"Memoria: {system_metrics['memory_percent']:.1f}%\n")
            f.write(f"Disco: {system_metrics['disk_usage_percent']:.1f}%\n\n")
            
            # Process metrics
            process_metrics = self.get_process_metrics()
            f.write("MÉTRICAS DEL PROCESO:\n")
            f.write(f"Memoria: {process_metrics['process_memory_mb']:.1f}MB\n")
            f.write(f"CPU: {process_metrics['process_cpu_percent']:.1f}%\n")
            f.write(f"Hilos: {process_metrics['num_threads']}\n\n")
            
            # Execution times
            if self.metrics:
                f.write("TIEMPOS DE EJECUCIÓN:\n")
                for operation, duration in self.metrics.items():
                    f.write(f"{operation}: {duration:.2f}s\n")
                
                total_time = sum(self.metrics.values())
                f.write(f"\nTiempo total: {total_time:.2f}s\n")
            
            # Performance score
            score = self.calculate_performance_score()
            f.write(f"\nScore de rendimiento: {score:.1f}/100\n")
        
        print(f"📄 Reporte exportado a: {report_path.absolute()}")
        return report_path


def mostrar_metricas_performance(metrics: Dict[str, Any]):
    """
    Show performance metrics in a formatted way.
    
    Args:
        metrics: Dictionary containing performance metrics
    """
    print("\n" + "=" * 60)
    print("📊 MÉTRICAS DE PERFORMANCE DEL PIPELINE")
    print("=" * 60)
    
    if not metrics:
        print("❌ No hay métricas disponibles.")
        return
    
    # Show timing metrics
    timing_metrics = {k: v for k, v in metrics.items() 
                     if isinstance(v, (int, float)) and k.endswith(('_time', '_duration', 'tiempo'))}
    
    if timing_metrics:
        print("\n⏱️ TIEMPOS DE EJECUCIÓN:")
        for metric, value in timing_metrics.items():
            print(f"   • {metric.replace('_', ' ').title()}: {value:.2f}s")
    
    # Show performance indicators
    perf_indicators = {k: v for k, v in metrics.items() 
                      if k in ['throughput', 'records_per_second', 'performance_score']}
    
    if perf_indicators:
        print("\n📈 INDICADORES DE RENDIMIENTO:")
        for indicator, value in perf_indicators.items():
            if indicator == 'performance_score':
                print(f"   • Score de performance: {value:.1f}/100")
            elif 'throughput' in indicator or 'records_per_second' in indicator:
                print(f"   • {indicator.replace('_', ' ').title()}: {value:,.0f} registros/seg")
            else:
                print(f"   • {indicator.replace('_', ' ').title()}: {value}")
    
    # Show memory and system metrics if available
    system_metrics = {k: v for k, v in metrics.items() 
                     if k in ['cpu_percent', 'memory_percent', 'memory_usage_mb']}
    
    if system_metrics:
        print("\n🖥️ MÉTRICAS DEL SISTEMA:")
        for metric, value in system_metrics.items():
            if 'percent' in metric:
                print(f"   • {metric.replace('_', ' ').title()}: {value:.1f}%")
            elif 'memory_usage_mb' in metric:
                print(f"   • Uso de memoria: {value:.1f}MB")
    
    # Calculate and show total time if individual times are available
    if timing_metrics:
        total_time = sum(timing_metrics.values())
        print(f"\n🎯 TIEMPO TOTAL: {total_time:.2f} segundos")
    
    # Show any errors or warnings
    if 'errors' in metrics and metrics['errors']:
        print(f"\n⚠️ ERRORES REGISTRADOS: {len(metrics['errors'])}")
        for error in metrics['errors'][:3]:  # Show first 3 errors
            print(f"   • {error}")


# Backward compatibility functions
def get_system_metrics():
    """Get system metrics for backward compatibility"""
    pm = PerformanceMetrics()
    return pm.get_system_metrics()

def calculate_performance_score():
    """Calculate performance score for backward compatibility"""
    pm = PerformanceMetrics()
    return pm.calculate_performance_score()
