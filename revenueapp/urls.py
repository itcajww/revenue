from django.contrib import admin
from django.urls import path, include
from . import views

urlpatterns = [
    path('', views.index,name='index'),
    path('dashboard/', views.dashboard,name='dashboard'),
    path('data_load/', views.data_load,name='data_load'),
    path('View_data/', views.View_data,name='View_data'),
    path('logout/',views.logout_view ,name='logout'),
    path('data_load_ajax/',views.data_load_ajax ,name='data_load_ajax'),
    path('data_load_uk_ajax/',views.data_load_uk_ajax ,name='data_load_uk_ajax'),

]
