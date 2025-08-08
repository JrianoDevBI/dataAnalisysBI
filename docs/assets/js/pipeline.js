/**
 * Pipeline de Análisis de Datos Inmobiliarios - JavaScript
 * Autor: Juan Camilo Riaño Molano
 * Fecha: 04/08/2025
 */

// Configuración inicial
const PipelineApp = {
    init: function() {
        this.initMermaid();
        this.bindEvents();
        this.initAnimations();
        this.initResultsSection();
    },

    // Inicializar Mermaid
    initMermaid: function() {
        mermaid.initialize({
            startOnLoad: true,
            theme: 'default',
            flowchart: {
                useMaxWidth: true,
                htmlLabels: true,
                curve: 'basis'
            },
            themeVariables: {
                primaryColor: '#2196F3',
                primaryTextColor: '#ffffff',
                primaryBorderColor: '#1976D2',
                lineColor: '#666666',
                secondaryColor: '#4CAF50',
                tertiaryColor: '#FF9800'
            }
        });
    },

    // Vincular eventos
    bindEvents: function() {
        // Eventos de las tarjetas de fase
        const phaseCards = document.querySelectorAll('.phase-card');
        phaseCards.forEach(card => {
            card.addEventListener('mouseenter', this.onPhaseCardHover.bind(this));
            card.addEventListener('mouseleave', this.onPhaseCardLeave.bind(this));
            card.addEventListener('click', this.onPhaseCardClick.bind(this));
        });

        // Eventos de botones de control
        const controlButtons = document.querySelectorAll('.control-btn');
        controlButtons.forEach(btn => {
            btn.addEventListener('click', this.onControlButtonClick.bind(this));
        });

        // Eventos de scroll suave
        this.initSmoothScroll();
    },

    // Inicializar sección de resultados
    initResultsSection: function() {
        // Animación de contador para métricas
        this.animateCounters();
        
        // Event listeners para enlaces de gráficos
        const chartLinks = document.querySelectorAll('.chart-link');
        chartLinks.forEach(link => {
            link.addEventListener('click', this.onChartLinkClick.bind(this));
        });

        // Event listeners para reportes
        const reportLinks = document.querySelectorAll('.report-link');
        reportLinks.forEach(link => {
            link.addEventListener('click', this.onReportLinkClick.bind(this));
        });

        // Lazy loading para imágenes de gráficos
        this.initLazyLoading();
    },

    // Animación de contadores
    animateCounters: function() {
        const indicators = document.querySelectorAll('.indicator-value');
        indicators.forEach(indicator => {
            const text = indicator.textContent;
            const numberMatch = text.match(/[\d,]+\.?\d*/);
            
            if (numberMatch) {
                const finalNumber = parseFloat(numberMatch[0].replace(',', ''));
                if (!isNaN(finalNumber)) {
                    this.animateNumber(indicator, 0, finalNumber, text);
                }
            }
        });
    },

    // Animar número específico
    animateNumber: function(element, start, end, originalText) {
        const duration = 2000;
        const increment = (end - start) / (duration / 16);
        let current = start;
        
        const timer = setInterval(() => {
            current += increment;
            if (current >= end) {
                current = end;
                clearInterval(timer);
            }
            
            const formattedNumber = Math.floor(current).toLocaleString();
            element.textContent = originalText.replace(/[\d,]+\.?\d*/, formattedNumber);
        }, 16);
    },

    // Manejar clics en enlaces de gráficos
    onChartLinkClick: function(event) {
        const link = event.target;
        
        // Agregar efecto visual
        link.style.transform = 'scale(0.95)';
        setTimeout(() => {
            link.style.transform = '';
        }, 150);

        // Log para analytics (opcional)
        console.log('Chart link clicked:', link.href);
    },

    // Manejar clics en enlaces de reportes
    onReportLinkClick: function(event) {
        const link = event.target;
        
        // Agregar efecto visual
        link.style.transform = 'scale(0.95)';
        setTimeout(() => {
            link.style.transform = '';
        }, 150);

        // Log para analytics (opcional)
        console.log('Report link clicked:', link.href);
    },

    // Inicializar lazy loading para imágenes
    initLazyLoading: function() {
        const images = document.querySelectorAll('.chart-image');
        
        if ('IntersectionObserver' in window) {
            const imageObserver = new IntersectionObserver((entries, observer) => {
                entries.forEach(entry => {
                    if (entry.isIntersecting) {
                        const img = entry.target;
                        img.style.opacity = '0';
                        img.style.transition = 'opacity 0.5s ease';
                        
                        setTimeout(() => {
                            img.style.opacity = '1';
                        }, 100);
                        
                        observer.unobserve(img);
                    }
                });
            });
            
            images.forEach(img => imageObserver.observe(img));
        }
    },

    // Inicializar animaciones
    initAnimations: function() {
        // Animación de entrada para las métricas
        const metricCards = document.querySelectorAll('.metric-card');
        metricCards.forEach((card, index) => {
            setTimeout(() => {
                card.classList.add('fade-in');
            }, index * 200);
        });

        // Animación de entrada para las fases
        const phaseCards = document.querySelectorAll('.phase-card');
        phaseCards.forEach((card, index) => {
            setTimeout(() => {
                card.classList.add('slide-in');
            }, index * 100);
        });
    },

    // Manejar hover en tarjetas de fase
    onPhaseCardHover: function(event) {
        const card = event.currentTarget;
        card.style.transform = 'translateY(-10px) scale(1.02)';
    },

    // Manejar leave en tarjetas de fase
    onPhaseCardLeave: function(event) {
        const card = event.currentTarget;
        card.style.transform = '';
    },

    // Manejar clic en tarjetas de fase
    onPhaseCardClick: function(event) {
        const card = event.currentTarget;
        const phase = this.getPhaseFromCard(card);
        this.scrollToDiagram(phase);
    },

    // Manejar clic en botones de control
    onControlButtonClick: function(event) {
        const button = event.currentTarget;
        const action = button.textContent.trim();
        
        if (action === '🔄 Reiniciar') {
            this.resetDiagram();
        } else {
            // Extraer la fase del texto del botón
            const phaseMap = {
                '📁': 'input',
                '🧹': 'preparation', 
                '🔍': 'analysis',
                '⚗️': 'treatment',
                '🧼': 'cleaning',
                '🗄️': 'database',
                '📊': 'reports'
            };
            
            const icon = action.charAt(0);
            const phase = phaseMap[icon];
            if (phase) {
                this.highlightPhase(phase);
            }
        }
    },

    // Obtener fase de la tarjeta
    getPhaseFromCard: function(card) {
        const classList = card.classList;
        const phaseClasses = ['entrada', 'preparacion', 'analisis', 'tratamiento', 'limpieza', 'database', 'reporteria'];
        return phaseClasses.find(cls => classList.contains(cls)) || 'input';
    },

    // Scroll suave al diagrama
    scrollToDiagram: function(phase) {
        const diagramSection = document.getElementById('diagram-section');
        if (diagramSection) {
            diagramSection.scrollIntoView({ 
                behavior: 'smooth',
                block: 'start'
            });
            
            // Resaltar la fase después del scroll
            setTimeout(() => {
                this.highlightPhase(phase);
            }, 800);
        }
    },

    // Inicializar scroll suave
    initSmoothScroll: function() {
        const links = document.querySelectorAll('a[href^="#"]');
        links.forEach(link => {
            link.addEventListener('click', function(e) {
                e.preventDefault();
                const target = document.querySelector(this.getAttribute('href'));
                if (target) {
                    target.scrollIntoView({
                        behavior: 'smooth'
                    });
                }
            });
        });
    }
};

// Funciones globales para el diagrama
function highlightPhase(phase) {
    console.log('Highlighting phase:', phase);
    // Implementar lógica de resaltado aquí
    updatePhaseInfo(phase);
}

function resetDiagram() {
    console.log('Resetting diagram');
    // Implementar lógica de reset aquí
    updatePhaseInfo('default');
}

function scrollToDiagram(phase) {
    PipelineApp.scrollToDiagram(phase);
}

function updatePhaseInfo(phase) {
    const infoPanel = document.getElementById('phase-info');
    if (!infoPanel) return;

    const phaseInfo = {
        input: {
            title: '📁 Entrada de Datos',
            description: 'Extracción y conversión de datos desde Excel a CSV.',
            metrics: 'Eficiencia: 6,544-10,471 registros/segundo'
        },
        preparation: {
            title: '🧹 Preparación de Datos',
            description: 'Backup inteligente y preparación del entorno.',
            metrics: 'Cache optimizado + Verificación completa'
        },
        analysis: {
            title: '🔍 Pre-Análisis',
            description: 'Análisis exhaustivo de inconsistencias y calidad.',
            metrics: 'Eficiencia: 4,363-6,544 registros/segundo'
        },
        treatment: {
            title: '⚗️ Tratamiento Estadístico',
            description: 'Deduplicación, imputación y winsorización avanzada.',
            metrics: 'Eficiencia: 349 outliers controlados'
        },
        cleaning: {
            title: '🧼 Limpieza Paralela',
            description: 'Procesamiento paralelo con fallback automático.',
            metrics: 'ThreadPool + Fallback garantizado'
        },
        database: {
            title: '🗄️ Base de Datos',
            description: 'Carga optimizada con pool de conexiones.',
            metrics: 'Eficiencia: 1,745-2,618 registros/segundo'
        },
        reports: {
            title: '📊 Reportería y Análisis',
            description: 'Generación de reportes y análisis exploratorio.',
            metrics: 'Múltiples reportes en batch'
        },
        default: {
            title: '📊 Información del Pipeline',
            description: 'Haga clic en cualquier fase para ver información detallada.',
            metrics: 'Pipeline Unificado - 47% de mejora en eficiencia'
        }
    };

    const info = phaseInfo[phase] || phaseInfo.default;
    
    infoPanel.innerHTML = `
        <h3>${info.title}</h3>
        <p>${info.description}</p>
        <p><strong>Métricas:</strong> ${info.metrics}</p>
    `;
}

// Inicializar cuando el DOM esté listo
document.addEventListener('DOMContentLoaded', function() {
    PipelineApp.init();
    console.log('Pipeline App initialized successfully');
});
